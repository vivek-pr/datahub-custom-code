import argparse
import fnmatch
import json
import logging
import os
import time
from typing import List, Optional, Set, Tuple

import psycopg
from psycopg import sql
from tenacity import retry, stop_after_attempt, wait_fixed

from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass, StringTypeClass

from actions.base64_action.configuration import (
    ActionConfig,
    DatasetIdentifier,
    RuntimeOverrides,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("base64-action")

class Base64EncodeAction:
    def __init__(self, config: ActionConfig) -> None:
        self.config = config
        self.graph = DataHubGraph(DatahubClientConfig(server=config.gms_url))
        self.conn = psycopg.connect(
            host=config.database.host,
            port=config.database.port,
            dbname=config.database.dbname,
            user=config.database.user,
            password=config.database.password,
            autocommit=False,
        )
        self._ensure_support_tables()

    def run(self) -> None:
        LOGGER.info(
            "Action ready. Watching DataHub runs for platform='%s' pipeline='%s' (database=%s, allowlist=%s).",
            self.config.platform,
            self.config.pipeline_name,
            self.config.database.dbname,
            ", ".join(self.config.schema_allowlist) or "<all>",
        )
        while True:
            try:
                self._process_pending_runs()
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.exception("Unexpected error while processing runs: %s", exc)
            time.sleep(self.config.poll_interval_seconds)

    def process_once(self) -> None:
        """Process the currently discovered datasets a single time."""
        LOGGER.info(
            "Processing datasets once for platform='%s' pipeline='%s' (database=%s, allowlist=%s).",
            self.config.platform,
            self.config.pipeline_name,
            self.config.database.dbname,
            ", ".join(self.config.schema_allowlist) or "<all>",
        )
        self._process_pending_runs()
        LOGGER.info("One-time processing complete.")

    def _ensure_support_tables(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS encoded")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS encoded._action_audit (
                    dataset_urn TEXT PRIMARY KEY,
                    ingestion_source_urn TEXT,
                    database_name TEXT,
                    table_schema TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    last_run_id TEXT,
                    row_count BIGINT NOT NULL,
                    checksum TEXT NOT NULL,
                    last_processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "ALTER TABLE encoded._action_audit ADD COLUMN IF NOT EXISTS ingestion_source_urn TEXT"
            )
            cur.execute(
                "ALTER TABLE encoded._action_audit ADD COLUMN IF NOT EXISTS database_name TEXT"
            )
        self.conn.commit()

    def _process_pending_runs(self, run_id: Optional[str] = None) -> None:
        try:
            urns = self._list_dataset_urns()
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error("Unable to list datasets for platform %s: %s", self.config.platform, exc)
            return

        if not urns:
            LOGGER.debug(
                "No datasets discovered for platform %s; will retry", self.config.platform
            )
            return

        run_id = run_id or f"scan-{int(time.time())}"
        LOGGER.info("Processing %d dataset(s) for run id %s", len(urns), run_id)

        for dataset_urn in urns:
            try:
                self._process_dataset(dataset_urn, run_id)
            except Exception as dataset_exc:  # pylint: disable=broad-except
                LOGGER.exception(
                    "Failed to process dataset %s during run %s: %s",
                    dataset_urn,
                    run_id,
                    dataset_exc,
                )

    def _list_dataset_urns(self) -> List[str]:
        batch_size = max(self.config.page_size, 25)
        start = 0
        urns: List[str] = []

        while True:
            batch = self.graph.list_all_entity_urns("dataset", start, batch_size) or []
            if not batch:
                break
            urns.extend(batch)
            if len(batch) < batch_size:
                break
            start += batch_size

        if not urns:
            return []

        platform_filter = f"urn:li:dataPlatform:{self.config.platform}" if self.config.platform else None
        if platform_filter:
            urns = [urn for urn in urns if platform_filter in urn]

        return urns

    def _process_dataset(self, dataset_urn: str, run_id: str) -> None:
        identifier = self._parse_dataset_urn(dataset_urn)
        if not identifier:
            LOGGER.warning("Could not parse dataset URN %s", dataset_urn)
            return
        if not self._dataset_matches_filters(identifier):
            LOGGER.debug(
                "Skipping dataset %s because it does not match filters (database=%s, allowlist=%s)",
                dataset_urn,
                self.config.database.dbname,
                ", ".join(self.config.schema_allowlist) or "<all>",
            )
            return
        schema_name, table_name = identifier.schema, identifier.table
        schema_metadata = self._get_schema_metadata(dataset_urn)
        if not schema_metadata:
            LOGGER.warning("No schema metadata available for %s", dataset_urn)
            return

        ordered_columns = self._fetch_table_columns(schema_name, table_name)
        if not ordered_columns:
            LOGGER.warning(
                "Source table %s.%s missing or has no columns; skipping",
                schema_name,
                table_name,
            )
            return

        text_columns = self._textual_columns_from_metadata(schema_metadata)
        LOGGER.info(
            "Encoding dataset %s => %s.%s (text columns: %s)",
            dataset_urn,
            schema_name,
            table_name,
            ", ".join(sorted(text_columns)) or "<none>",
        )
        checksum, row_count = self._compute_source_checksum(schema_name, table_name)
        if checksum is None or row_count is None:
            LOGGER.warning("Unable to compute checksum for %s", dataset_urn)
            return

        if self._should_skip_dataset(dataset_urn, checksum, row_count):
            LOGGER.info("No data change detected for %s; skipping", dataset_urn)
            return

        self._apply_encoding(schema_name, table_name, ordered_columns, text_columns)
        self._record_dataset_state(
            dataset_urn,
            identifier,
            run_id,
            checksum,
            row_count,
        )
        LOGGER.info(
            "Finished encoding %s (%d rows)",
            dataset_urn,
            row_count,
        )

    def _dataset_matches_filters(self, identifier: DatasetIdentifier) -> bool:
        database_filter = (self.config.database.dbname or "").lower()
        if database_filter and identifier.database.lower() != database_filter:
            return False
        if self.config.schema_allowlist:
            schema_table = identifier.schema_table.lower()
            if not any(fnmatch.fnmatch(schema_table, pattern) for pattern in self.config.schema_allowlist):
                return False
        return True

    def _should_skip_dataset(self, dataset_urn: str, checksum: str, row_count: int) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT row_count, checksum FROM encoded._action_audit WHERE dataset_urn = %s",
                (dataset_urn,),
            )
            existing = cur.fetchone()
        if not existing:
            return False
        existing_row_count, existing_checksum = existing
        return existing_row_count == row_count and existing_checksum == checksum

    def _record_dataset_state(
        self,
        dataset_urn: str,
        identifier: DatasetIdentifier,
        run_id: str,
        checksum: str,
        row_count: int,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO encoded._action_audit (
                    dataset_urn,
                    ingestion_source_urn,
                    database_name,
                    table_schema,
                    table_name,
                    last_run_id,
                    row_count,
                    checksum,
                    last_processed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (dataset_urn)
                DO UPDATE SET
                    last_run_id = EXCLUDED.last_run_id,
                    row_count = EXCLUDED.row_count,
                    checksum = EXCLUDED.checksum,
                    last_processed_at = NOW()
                """,
                (
                    dataset_urn,
                    self.config.pipeline_name,
                    identifier.database,
                    identifier.schema,
                    identifier.table,
                    run_id,
                    row_count,
                    checksum,
                ),
            )
        self.conn.commit()

    def _apply_encoding(
        self,
        schema_name: str,
        table_name: str,
        ordered_columns: List[str],
        text_columns: Set[str],
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS encoded")
            cur.execute(
                sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {encoded_schema}.{encoded_table}
                    (LIKE {source_schema}.{source_table} INCLUDING ALL)
                """)
                .format(
                    encoded_schema=sql.Identifier("encoded"),
                    encoded_table=sql.Identifier(table_name),
                    source_schema=sql.Identifier(schema_name),
                    source_table=sql.Identifier(table_name),
                )
            )
            cur.execute(
                sql.SQL("TRUNCATE TABLE {encoded_schema}.{encoded_table}")
                .format(
                    encoded_schema=sql.Identifier("encoded"),
                    encoded_table=sql.Identifier(table_name),
                )
            )
            select_clause = self._build_select_clause(ordered_columns, text_columns)
            insert_stmt = (
                sql.SQL("INSERT INTO {encoded_schema}.{encoded_table} ({columns}) SELECT {select_clause} FROM {source_schema}.{source_table}")
                .format(
                    encoded_schema=sql.Identifier("encoded"),
                    encoded_table=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(sql.Identifier(col) for col in ordered_columns),
                    select_clause=select_clause,
                    source_schema=sql.Identifier(schema_name),
                    source_table=sql.Identifier(table_name),
                )
            )
            cur.execute(insert_stmt)
        self.conn.commit()

    def _build_select_clause(self, columns: List[str], text_columns: Set[str]) -> sql.SQL:
        expressions: List[sql.SQL] = []
        for column in columns:
            ident = sql.Identifier(column)
            if column.lower() in text_columns:
                expressions.append(
                    sql.SQL(
                        "CASE WHEN {col} IS NULL THEN NULL ELSE encode(convert_to({col}::text, 'UTF8'), 'base64') END AS {alias}"
                    ).format(col=ident, alias=ident)
                )
            else:
                expressions.append(sql.SQL("{col}").format(col=ident))
        return sql.SQL(", ").join(expressions)

    def _fetch_table_columns(self, schema_name: str, table_name: str) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, table_name),
            )
            rows = cur.fetchall()
        return [row[0] for row in rows] if rows else []

    def _compute_source_checksum(self, schema_name: str, table_name: str) -> Tuple[Optional[str], Optional[int]]:
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    WITH row_hashes AS (
                        SELECT md5(row_to_json(t)::text) AS row_hash
                        FROM {schema}.{table} AS t
                    )
                    SELECT
                        COALESCE(md5(string_agg(row_hash, '' ORDER BY row_hash)), '0') AS checksum,
                        (SELECT COUNT(*) FROM {schema}.{table}) AS row_count
                    FROM row_hashes
                    """
                ).format(
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table_name),
                )
            )
            result = cur.fetchone()
        if not result:
            return None, None
        return result[0], result[1]

    def _get_schema_metadata(self, dataset_urn: str) -> Optional[SchemaMetadataClass]:
        try:
            return self.graph.get_schema_metadata(dataset_urn)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error("Unable to load schema metadata for %s: %s", dataset_urn, exc)
            return None

    @staticmethod
    def _textual_columns_from_metadata(schema_metadata: SchemaMetadataClass) -> Set[str]:
        text_columns: Set[str] = set()
        for field in schema_metadata.fields or []:  # type: ignore[attr-defined]
            column = Base64EncodeAction._normalize_field_path(field)
            if not column:
                continue
            if Base64EncodeAction._is_textual(field):
                text_columns.add(column.lower())
        return text_columns

    @staticmethod
    def _is_textual(field: SchemaFieldClass) -> bool:
        native = (field.nativeDataType or "").lower()
        if "char" in native or "text" in native:
            return True
        type_class = getattr(field.type, "type", None)
        if isinstance(type_class, StringTypeClass):
            return True
        return False

    @staticmethod
    def _normalize_field_path(field: SchemaFieldClass) -> Optional[str]:
        path = field.fieldPath or ""
        if not path:
            return None
        candidate = path.split(".")[-1]
        return candidate.strip('"')

    @staticmethod
    def _parse_dataset_urn(dataset_urn: str) -> Optional[DatasetIdentifier]:
        try:
            inner = dataset_urn.split("(", 1)[1].rstrip(")")
            parts = inner.split(",")
            if len(parts) < 2:
                return None
            dataset_name = parts[1]
            name_parts = dataset_name.split(".")
            if len(name_parts) < 2:
                return None
            database_name = name_parts[0]
            schema_name = name_parts[-2]
            table_name = name_parts[-1]
            return DatasetIdentifier(dataset_urn, database_name, schema_name, table_name)
        except Exception:  # pylint: disable=broad-except
            return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Base64 encoding action")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process the currently available datasets once and then exit",
    )
    args = parser.parse_args()

    overrides = RuntimeOverrides()
    action_config = ActionConfig.load(overrides=overrides)
    LOGGER.info(
        "Loaded action config (gms_url=%s, platform=%s, pipeline_name=%s)",
        action_config.gms_url,
        action_config.platform,
        action_config.pipeline_name,
    )
    action = Base64EncodeAction(action_config)
    try:
        if args.once:
            action.process_once()
        else:
            action.run()
    finally:
        action.conn.close()
