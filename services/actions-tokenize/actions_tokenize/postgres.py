"""Postgres utilities for running tokenization updates."""

from __future__ import annotations

import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

import psycopg2
from psycopg2 import sql

from .config import PostgresSettings, TenantCredential

LOGGER = logging.getLogger(__name__)

_TEXTUAL_DATA_TYPES = {"text", "character varying", "character", "name"}
_TEXTUAL_UDT_NAMES = {"varchar", "text", "bpchar", "citext"}


@dataclass(frozen=True)
class TokenizeColumn:
    """Column selected for tokenization."""

    field_path: str
    column: str
    pii_tags: Sequence[str]


@dataclass
class ColumnTokenizationPlan:
    column: TokenizeColumn
    rows_to_update: int
    data_type: Optional[str]


@dataclass
class TokenizationOutcome:
    total_rows: int
    per_column: List[ColumnTokenizationPlan]


class PostgresExecutor:
    """Execute tokenization statements using tenant-scoped credentials."""

    def __init__(self, settings: PostgresSettings, logger: logging.Logger = LOGGER):
        self._settings = settings
        self._logger = logger
        self._tenant_lookup: Dict[str, TenantCredential] = {
            cred.tenant_id: cred for cred in settings.tenants
        }

    def _resolve_credentials(self, tenant_id: str) -> TenantCredential:
        if tenant_id not in self._tenant_lookup:
            raise KeyError(
                f"No Postgres credentials configured for tenant '{tenant_id}'."
            )
        return self._tenant_lookup[tenant_id]

    @contextlib.contextmanager
    def connection(self, tenant_id: str):
        cred = self._resolve_credentials(tenant_id)
        username = cred.username or cred.tenant_id
        password = cred.password.get_secret_value()
        conn = psycopg2.connect(
            host=self._settings.host,
            port=self._settings.port,
            dbname=self._settings.database,
            user=username,
            password=password,
            connect_timeout=self._settings.connect_timeout_seconds,
            application_name=self._settings.application_name,
            sslmode=self._settings.sslmode,
        )
        try:
            search_path = cred.search_path or self._settings.default_search_path
            if search_path:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SET search_path = {}").format(sql.SQL(search_path)))
            if cred.role:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SET ROLE {}" ).format(sql.Identifier(cred.role)))
            yield conn
        finally:
            conn.close()

    def _is_textual_column(
        self, conn, schema: str, table: str, column: str
    ) -> tuple[bool, Optional[str]]:
        query = sql.SQL(
            """
            SELECT data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """
        )
        with conn.cursor() as cur:
            cur.execute(query, (schema, table, column))
            row = cur.fetchone()
        if row is None:
            raise ValueError(
                f"Column metadata not found for {schema}.{table}.{column}."
            )
        data_type, udt_name = row
        textual = (data_type in _TEXTUAL_DATA_TYPES) or (
            udt_name in _TEXTUAL_UDT_NAMES
        )
        return textual, data_type

    def _count_rows_to_tokenize(
        self,
        conn,
        schema: str,
        table: str,
        column: str,
        token_pattern: str,
    ) -> int:
        table_identifier = sql.Identifier(schema, table)
        column_identifier = sql.Identifier(column)
        query = sql.SQL(
            """
            SELECT COUNT(*)
            FROM {table}
            WHERE {column} IS NOT NULL
              AND ({column})::text !~ %s
            """
        ).format(table=table_identifier, column=column_identifier)
        with conn.cursor() as cur:
            cur.execute(query, (token_pattern,))
            (count,) = cur.fetchone()
        return int(count)

    def _make_token_expression(self, column: str, use_base64: bool) -> sql.SQL:
        column_identifier = sql.Identifier(column)
        column_text = sql.SQL("({col})::text").format(col=column_identifier)
        if use_base64:
            # Remove embedded newlines emitted by encode(..., 'base64').
            return sql.SQL(
                "( 'tok_' || replace(encode(convert_to({column_text}, 'UTF8'), 'base64'), E'\n', '') || '_poc' )"
            ).format(column_text=column_text)
        # Fallback to md5 to avoid pgcrypto dependency.
        return sql.SQL(
            "( 'tok_' || md5({column_text}) || '_poc' )"
        ).format(column_text=column_text)

    def _execute_update(
        self,
        conn,
        schema: str,
        table: str,
        column: str,
        token_pattern: str,
        use_base64: bool,
        dry_run: bool,
    ) -> int:
        table_identifier = sql.Identifier(schema, table)
        column_identifier = sql.Identifier(column)
        token_expr = self._make_token_expression(column, use_base64)
        update_stmt = sql.SQL(
            """
            UPDATE {table}
               SET {column} = {token_expr}
             WHERE {column} IS NOT NULL
               AND ({column})::text !~ %s
            """
        ).format(
            table=table_identifier,
            column=column_identifier,
            token_expr=token_expr,
        )
        if dry_run:
            self._logger.info(
                json.dumps(
                    {
                        "event": "postgres.dry_run",
                        "schema": schema,
                        "table": table,
                        "column": column,
                    }
                )
            )
            return 0
        with conn.cursor() as cur:
            cur.execute(update_stmt, (token_pattern,))
            return cur.rowcount

    def tokenize(
        self,
        tenant_id: str,
        schema: str,
        table: str,
        columns: Sequence[TokenizeColumn],
        token_pattern: str,
        use_base64: bool,
        dry_run: bool = False,
    ) -> TokenizationOutcome:
        per_column: List[ColumnTokenizationPlan] = []
        total_rows = 0

        with self.connection(tenant_id) as conn:
            conn.autocommit = False
            try:
                for column in columns:
                    is_textual, data_type = self._is_textual_column(
                        conn, schema, table, column.column
                    )
                    if not is_textual:
                        self._logger.warning(
                            json.dumps(
                                {
                                    "event": "postgres.unsupported_column_type",
                                    "schema": schema,
                                    "table": table,
                                    "column": column.column,
                                    "data_type": data_type,
                                    "reason": "non-textual column skipped",
                                }
                            )
                        )
                        per_column.append(
                            ColumnTokenizationPlan(
                                column=column,
                                rows_to_update=0,
                                data_type=data_type,
                            )
                        )
                        continue
                    rows_to_update = self._count_rows_to_tokenize(
                        conn, schema, table, column.column, token_pattern
                    )
                    total_rows += rows_to_update
                    if rows_to_update > 0:
                        updated_rows = self._execute_update(
                            conn,
                            schema,
                            table,
                            column.column,
                            token_pattern,
                            use_base64,
                            dry_run,
                        )
                        # When dry_run is False, updated_rows should match rows_to_update.
                        if not dry_run:
                            total_rows -= rows_to_update
                            total_rows += updated_rows
                            rows_to_update = updated_rows
                    per_column.append(
                        ColumnTokenizationPlan(
                            column=column,
                            rows_to_update=rows_to_update,
                            data_type=data_type,
                        )
                    )
                if dry_run:
                    conn.rollback()
                else:
                    conn.commit()
            except Exception:
                conn.rollback()
                raise

        return TokenizationOutcome(total_rows=total_rows, per_column=per_column)
