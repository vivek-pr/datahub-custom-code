"""Coordinator that executes tokenization runs and reports status."""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Optional, Sequence, Set

from . import db_dbx, db_pg
from .datahub_client import DataHubClient, RUN_TAG_URN
from .models import DatasetMetadata, RunStatus, TokenizationResult
from .pii_detector import PiiDetector
from .sdk_adapter import TokenizationSDKAdapter

LOGGER = logging.getLogger(__name__)

DONE_TAG_URN = "urn:li:tag:tokenize/done"
STATUS_SUCCESS_TAG = "urn:li:tag:tokenize/status:SUCCESS"
STATUS_FAILED_TAG = "urn:li:tag:tokenize/status:FAILED"
FIELD_TOKENIZED_TAG = "urn:li:tag:tokenized"


class RunManager:
    """Orchestrates tokenization runs and DataHub updates."""

    def __init__(
        self,
        client: DataHubClient,
        detector: PiiDetector,
        adapter: TokenizationSDKAdapter,
        *,
        batch_limit: int = 100,
    ) -> None:
        self.client = client
        self.detector = detector
        self.adapter = adapter
        self.batch_limit = batch_limit
        self.pg_conn_str = os.environ.get("PG_CONN_STR")
        self.dbx_jdbc_url = os.environ.get("DBX_JDBC_URL")

    # ------------------------------------------------------------------
    def process(
        self,
        dataset: DatasetMetadata,
        explicit_fields: Optional[Sequence[str]] = None,
    ) -> None:
        run_id = str(uuid.uuid4())
        started_at = datetime.utcnow()
        columns: Sequence[str] = []
        try:
            columns = sorted(
                self.detector.detect(dataset, explicit_fields=explicit_fields)
            )
            LOGGER.info(
                "Tokenization run %s on %s targeting columns=%s",
                run_id,
                dataset.urn,
                columns,
            )
            result = self._execute_tokenization(dataset, columns)
            message = self._success_message(result)
            status = RunStatus(
                run_id=run_id,
                started_at=started_at,
                ended_at=datetime.utcnow(),
                platform=dataset.platform,
                columns=list(columns),
                rows_updated=result.rows_updated,
                rows_skipped=result.rows_skipped,
                status="SUCCESS",
                message=message,
            )
            self._record_status(dataset, status)
            self._finalise_success(dataset, columns)
        except Exception as exc:
            LOGGER.exception("Tokenization failed for %s", dataset.urn)
            status = RunStatus(
                run_id=run_id,
                started_at=started_at,
                ended_at=datetime.utcnow(),
                platform=dataset.platform,
                columns=list(columns),
                rows_updated=0,
                rows_skipped=0,
                status="FAILED",
                message=str(exc),
            )
            self._record_status(dataset, status)
            self._mark_failure(dataset)

    # ------------------------------------------------------------------
    def _execute_tokenization(
        self, dataset: DatasetMetadata, columns: Sequence[str]
    ) -> TokenizationResult:
        if not columns:
            return TokenizationResult(columns=columns, rows_updated=0, rows_skipped=0)
        if dataset.platform == "postgres":
            if not self.pg_conn_str:
                raise RuntimeError(
                    "PG_CONN_STR must be configured for Postgres datasets"
                )
            return db_pg.tokenize_table(
                self.pg_conn_str, dataset.ref, columns, self.batch_limit, self.adapter
            )
        if dataset.platform == "databricks":
            if not self.dbx_jdbc_url:
                raise RuntimeError(
                    "DBX_JDBC_URL must be configured for Databricks datasets"
                )
            return db_dbx.tokenize_table(
                self.dbx_jdbc_url, dataset.ref, columns, self.batch_limit, self.adapter
            )
        raise RuntimeError(f"Unsupported platform: {dataset.platform}")

    def _success_message(self, result: TokenizationResult) -> str:
        if not result.columns:
            return "No PII columns detected; nothing to tokenise."
        return (
            "Tokenised columns {columns}; updated {updated} rows, skipped {skipped}."
        ).format(
            columns=", ".join(result.columns),
            updated=result.rows_updated,
            skipped=result.rows_skipped,
        )

    # ------------------------------------------------------------------
    def _record_status(self, dataset: DatasetMetadata, status: RunStatus) -> None:
        payload = status.dict()
        payload["started_at"] = status.started_at.isoformat()
        payload["ended_at"] = status.ended_at.isoformat() if status.ended_at else None
        properties = dict(dataset.editable_properties)
        properties["last_tokenization_run"] = json.dumps(payload)
        self.client.upsert_editable_properties(dataset.urn, properties)

    def _finalise_success(
        self, dataset: DatasetMetadata, columns: Sequence[str]
    ) -> None:
        LOGGER.info("Finalising successful run for %s", dataset.urn)
        self.client.ensure_tags(
            dataset.urn,
            tags_to_add={DONE_TAG_URN, STATUS_SUCCESS_TAG},
            tags_to_remove={RUN_TAG_URN, STATUS_FAILED_TAG},
        )
        field_map = dataset.field_map()
        processed_fields: Set[str] = set()
        for field in dataset.fields:
            additions: Set[str] = set()
            removals: Set[str] = set()
            if field.column in columns:
                additions.add(FIELD_TOKENIZED_TAG)
                processed_fields.add(field.field_path)
            if RUN_TAG_URN in field.tags:
                removals.add(RUN_TAG_URN)
            if additions or removals:
                self.client.ensure_tags(
                    dataset.urn,
                    tags_to_add=additions,
                    tags_to_remove=removals,
                    field_path=field.field_path,
                )
        for column in columns:
            field = field_map.get(column)
            if not field:
                continue
            if field.field_path in processed_fields:
                continue
            self.client.ensure_tags(
                dataset.urn,
                tags_to_add={FIELD_TOKENIZED_TAG},
                tags_to_remove=set(),
                field_path=field.field_path,
            )

    def _mark_failure(self, dataset: DatasetMetadata) -> None:
        LOGGER.info("Marking run failure for %s", dataset.urn)
        self.client.ensure_tags(
            dataset.urn,
            tags_to_add={STATUS_FAILED_TAG},
            tags_to_remove={STATUS_SUCCESS_TAG},
        )
