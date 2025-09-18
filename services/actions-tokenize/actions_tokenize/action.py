"""Tokenize action implementation for DataHub Actions."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, cast

from pydantic import ValidationError

from datahub.emitter.mce_builder import (
    dataset_urn_to_key,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_data_process_instance_urn,
    schema_field_urn_to_key,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataFlowInfoClass,
    DataFlowKeyClass,
    DataJobInfoClass,
    DataJobKeyClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceKeyClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GenericAspectClass,
    GlobalTagsClass,
    MetadataChangeLogClass,
    RunResultTypeClass,
)
from datahub.metadata.urns import Urn

try:  # pragma: no cover - datahub_actions only available inside the worker image
    from datahub_actions.context.action_context import ActionContext
    from datahub_actions.event.event_envelope import EventEnvelope, EventType
    from datahub_actions.plugin.action_base import Action
except ImportError:  # pragma: no cover - local unit tests
    ActionContext = Any  # type: ignore
    EventEnvelope = Any  # type: ignore

    class _EventTypeFallback:
        METADATA_CHANGE_LOG_EVENT = "METADATA_CHANGE_LOG_EVENT"

    EventType = _EventTypeFallback  # type: ignore

    class Action:  # type: ignore
        """Fallback Action base when datahub_actions is unavailable."""

        @classmethod
        def create(cls, config_dict: Dict[str, Any], ctx: Any) -> "Action":
            raise NotImplementedError


from .config import TokenizeActionConfig
from .postgres import PostgresExecutor, TokenizeColumn, TokenizationOutcome

LOGGER = logging.getLogger(__name__)


@dataclass
class RunHandle:
    """Book-keeping for an in-flight tokenization run."""

    run_id: str
    run_urn: str
    started_ts_ms: int
    external_url: Optional[str]
    created_audit: AuditStampClass
    base_custom_properties: Dict[str, str]
    column_context: List[Dict[str, Any]]


@dataclass
class EventContext:
    """Resolved context extracted from a tag change event."""

    dataset_urn: str
    dataset_name: str
    platform: str
    schema: str
    table: str
    tenant_id: str
    field_path: Optional[str]
    new_tags: Sequence[str]
    previous_tags: Sequence[str]


class TokenizeAction(Action):
    """DataHub Action that tokenizes PII columns in Postgres on demand."""

    def __init__(self, config: TokenizeActionConfig, ctx: Optional[ActionContext] = None):
        self._config = config
        self._logger = LOGGER
        self._graph = self._resolve_graph(ctx)
        self._postgres = PostgresExecutor(config.postgres, logger=self._logger)
        self._ensured_metadata = False
        self._data_flow_urn = make_data_flow_urn(
            config.dataflow.orchestrator,
            config.dataflow.flow_id,
            config.dataflow.env,
        )
        self._data_job_urn = make_data_job_urn_with_flow(
            self._data_flow_urn, config.datajob.job_id
        )

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: Optional[ActionContext]) -> "TokenizeAction":
        try:
            config = TokenizeActionConfig.parse_obj(config_dict or {})
        except ValidationError as exc:  # pragma: no cover - config validation occurs at boot
            raise ValueError(f"Invalid TokenizeAction configuration: {exc}") from exc
        return cls(config, ctx)

    @property  # pragma: no cover - exercised by actions runtime
    def name(self) -> str:
        return "tokenize-action"

    def close(self) -> None:  # pragma: no cover - no resources today
        """Hook to satisfy Action interface."""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def act(self, event: EventEnvelope, action_context: Optional[ActionContext] = None) -> None:
        """Entry point invoked by the DataHub Actions runtime."""

        mcl = self._extract_change_log(event)
        if mcl is None:
            self._log("event.skipped", reason="not_metadata_change_log")
            return
        if not self._is_tag_update(mcl):
            self._log("event.skipped", reason="not_global_tags")
            return

        try:
            ctx = self._resolve_event_context(mcl)
        except ValueError as exc:
            self._log("event.skipped", reason=str(exc))
            return

        if self._config.token_tag not in ctx.new_tags:
            self._log(
                "event.skipped",
                reason="token_tag_not_present",
                dataset_urn=ctx.dataset_urn,
            )
            return
        if self._config.token_tag in ctx.previous_tags:
            self._log(
                "event.skipped",
                reason="token_tag_already_present",
                dataset_urn=ctx.dataset_urn,
            )
            return

        columns = self._resolve_columns(ctx.dataset_urn, ctx.field_path)
        if not columns:
            self._log(
                "event.skipped",
                reason="no_pii_columns",
                dataset_urn=ctx.dataset_urn,
                field_path=ctx.field_path,
            )
            return

        if (
            self._config.max_columns is not None
            and len(columns) > self._config.max_columns
        ):
            raise RuntimeError(
                f"Column safety limit exceeded: {len(columns)} > {self._config.max_columns}"
            )

        self._ensure_metadata()

        external_url = self._build_external_url(ctx.dataset_urn)
        run_handle = self._start_run(ctx, columns, external_url)

        outcome: Optional[TokenizationOutcome] = None
        error: Optional[Exception] = None
        started = run_handle.started_ts_ms
        try:
            outcome = self._postgres.tokenize(
                tenant_id=ctx.tenant_id,
                schema=ctx.schema,
                table=ctx.table,
                columns=columns,
                token_pattern=self._config.regex_token_pattern,
                use_base64=self._config.use_base64_tokens,
                dry_run=self._config.dry_run,
            )
            self._log(
                "tokenize.success",
                dataset_urn=ctx.dataset_urn,
                tenant_id=ctx.tenant_id,
                rows_affected=outcome.total_rows,
            )
        except Exception as exc:  # noqa: BLE001 - propagate after recording failure
            error = exc
            self._log(
                "tokenize.error",
                dataset_urn=ctx.dataset_urn,
                tenant_id=ctx.tenant_id,
                error=str(exc),
            )
        finally:
            self._complete_run(
                run_handle,
                started_ms=started,
                outcome=outcome,
                error=error,
                columns=columns,
                event_ctx=ctx,
            )

        if error is not None:
            raise error

    # ------------------------------------------------------------------
    # Metadata orchestration
    # ------------------------------------------------------------------

    def _ensure_metadata(self) -> None:
        if self._ensured_metadata:
            return

        flow_name = self._config.dataflow.name or self._config.dataflow.flow_id
        flow_custom_props = (
            dict(self._config.dataflow.custom_properties)
            if self._config.dataflow.custom_properties
            else None
        )
        job_name = self._config.datajob.name or self._config.datajob.job_id
        job_custom_props = (
            dict(self._config.datajob.custom_properties)
            if self._config.datajob.custom_properties
            else None
        )

        aspects = [
            (
                self._data_flow_urn,
                DataFlowKeyClass(
                    orchestrator=self._config.dataflow.orchestrator,
                    flowId=self._config.dataflow.flow_id,
                    cluster=self._config.dataflow.env,
                ),
            ),
            (
                self._data_flow_urn,
                DataFlowInfoClass(
                    name=flow_name,
                    description=self._config.dataflow.description,
                    project=self._config.dataflow.flow_id,
                    externalUrl=self._config.dataflow.external_url,
                    customProperties=flow_custom_props,
                    env=self._config.dataflow.env,
                ),
            ),
            (
                self._data_job_urn,
                DataJobKeyClass(
                    flow=self._data_flow_urn,
                    jobId=self._config.datajob.job_id,
                ),
            ),
            (
                self._data_job_urn,
                DataJobInfoClass(
                    name=job_name,
                    type="TASK",
                    description=self._config.datajob.description,
                    externalUrl=self._config.datajob.external_url,
                    customProperties=job_custom_props,
                    flowUrn=self._data_flow_urn,
                    env=self._config.dataflow.env,
                ),
            ),
        ]

        for urn, aspect in aspects:
            if aspect is None:
                continue
            mcp = self._mcp(urn, aspect)
            self._emit(mcp)

        self._ensured_metadata = True

    def _start_run(
        self,
        event_ctx: EventContext,
        columns: Sequence[TokenizeColumn],
        external_url: Optional[str],
    ) -> RunHandle:
        run_id = self._build_run_id(event_ctx.dataset_name)
        run_urn = make_data_process_instance_urn(run_id)
        started_ts = self._now_ms()
        audit = self._make_audit_stamp(timestamp=started_ts)
        base_custom_properties = {
            "dataset_urn": event_ctx.dataset_urn,
            "dataset_name": event_ctx.dataset_name,
            "tenant_id": event_ctx.tenant_id,
            "schema": event_ctx.schema,
            "table": event_ctx.table,
            "token_tag": self._config.token_tag,
            "trigger_scope": "column" if event_ctx.field_path else "dataset",
            "dry_run": str(self._config.dry_run).lower(),
        }
        column_summary = [
            {
                "fieldPath": column.field_path,
                "column": column.column,
                "piiTags": list(column.pii_tags),
            }
            for column in columns
        ]
        base_custom_properties["columns"] = json.dumps(column_summary, sort_keys=True)
        base_custom_properties.update(self._config.runtime_custom_properties)

        properties = DataProcessInstancePropertiesClass(
            name=f"Tokenize {event_ctx.dataset_name}",
            created=audit,
            externalUrl=external_url,
            customProperties=base_custom_properties,
        )

        inputs = DataProcessInstanceInputClass(inputs=[event_ctx.dataset_urn])
        outputs = DataProcessInstanceOutputClass(outputs=[event_ctx.dataset_urn])
        relationships = DataProcessInstanceRelationshipsClass(
            upstreamInstances=[],
            parentTemplate=self._data_job_urn,
        )

        for aspect in (
            DataProcessInstanceKeyClass(id=run_id),
            properties,
            inputs,
            outputs,
            relationships,
        ):
            self._emit(self._mcp(run_urn, aspect))

        run_event = DataProcessInstanceRunEventClass(
            timestampMillis=started_ts,
            status=DataProcessRunStatusClass.STARTED,
            externalUrl=external_url,
        )
        self._emit(self._mcp(run_urn, run_event))

        self._log(
            "run.started",
            run_urn=run_urn,
            dataset_urn=event_ctx.dataset_urn,
            columns=len(columns),
            tenant_id=event_ctx.tenant_id,
        )

        return RunHandle(
            run_id=run_id,
            run_urn=run_urn,
            started_ts_ms=started_ts,
            external_url=external_url,
            created_audit=audit,
            base_custom_properties=base_custom_properties,
            column_context=column_summary,
        )

    def _complete_run(
        self,
        handle: RunHandle,
        *,
        started_ms: int,
        outcome: Optional[TokenizationOutcome],
        error: Optional[Exception],
        columns: Sequence[TokenizeColumn],
        event_ctx: EventContext,
    ) -> None:
        finished = self._now_ms()
        duration = max(0, finished - started_ms)
        success = error is None
        rows_total = outcome.total_rows if outcome else 0
        per_column = outcome.per_column if outcome else []

        per_column_map = {
            plan.column.field_path: plan.rows_to_update for plan in per_column
        }
        columns_with_counts: List[Dict[str, Any]] = []
        for entry in handle.column_context:
            updated_entry = dict(entry)
            field_path = updated_entry.get("fieldPath")
            updated_entry["rowsAffected"] = per_column_map.get(field_path, 0)
            columns_with_counts.append(updated_entry)

        custom_properties = dict(handle.base_custom_properties)
        custom_properties.update(
            {
                "rows_affected_total": str(rows_total),
                "status": "SUCCESS" if success else "FAILURE",
                "completed_at": str(finished),
            }
        )
        custom_properties["columns"] = json.dumps(columns_with_counts, sort_keys=True)

        if per_column_map:
            custom_properties["rows_affected_per_column"] = json.dumps(
                per_column_map, sort_keys=True
            )

        if error is not None:
            custom_properties["error"] = str(error)

        result_payload = {
            "datasetUrn": event_ctx.dataset_urn,
            "tenantId": event_ctx.tenant_id,
            "rowsAffected": rows_total,
            "dryRun": self._config.dry_run,
            "columns": columns_with_counts,
            "status": "SUCCESS" if success else "FAILURE",
        }
        if error is not None:
            result_payload["error"] = str(error)
        custom_properties["run_result"] = json.dumps(result_payload, sort_keys=True)

        properties = DataProcessInstancePropertiesClass(
            name=f"Tokenize {event_ctx.dataset_name}",
            created=handle.created_audit,
            externalUrl=handle.external_url,
            customProperties=custom_properties,
        )
        self._emit(self._mcp(handle.run_urn, properties))

        result = DataProcessInstanceRunResultClass(
            type=RunResultTypeClass.SUCCESS if success else RunResultTypeClass.FAILURE,
            nativeResultType="actions-tokenize",
        )
        run_event = DataProcessInstanceRunEventClass(
            timestampMillis=finished,
            status=DataProcessRunStatusClass.COMPLETE,
            externalUrl=handle.external_url,
            result=result,
            durationMillis=duration,
        )
        self._emit(self._mcp(handle.run_urn, run_event))

        self._log(
            "run.completed",
            run_urn=handle.run_urn,
            status="SUCCESS" if success else "FAILURE",
            rows_affected=rows_total,
            duration_ms=duration,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_graph(self, ctx: Optional[ActionContext]) -> DataHubGraph:
        graph = getattr(ctx, "graph", None)
        if graph is not None:
            return cast(DataHubGraph, graph)
        server = (
            os.getenv("DATAHUB_GMS_URL")
            or os.getenv("DATAHUB_GMS")
        )
        if not server:
            raise RuntimeError("DataHub graph client unavailable; set DATAHUB_GMS_URL")
        token = os.getenv("DATAHUB_GMS_TOKEN")
        config = DatahubClientConfig(server=server, token=token)
        return DataHubGraph(config)

    def _extract_change_log(self, envelope: EventEnvelope) -> Optional[MetadataChangeLogClass]:
        event_type = getattr(envelope, "event_type", None)
        if event_type is not None:
            event_type_value = getattr(event_type, "value", None)
            if event_type_value is None:
                event_type_value = str(event_type)
            if "METADATA_CHANGE_LOG" not in str(event_type_value):
                return None
        event = getattr(envelope, "event", envelope)
        if isinstance(event, MetadataChangeLogClass):
            return event
        if isinstance(event, dict):
            try:
                return MetadataChangeLogClass.from_obj(event)
            except Exception:  # pragma: no cover - defensive fallback
                return None
        return None

    @staticmethod
    def _is_tag_update(mcl: MetadataChangeLogClass) -> bool:
        return mcl.aspectName == "globalTags"

    def _resolve_event_context(self, mcl: MetadataChangeLogClass) -> EventContext:
        dataset_urn: str
        field_path: Optional[str] = None

        if mcl.entityType == "dataset":
            if not mcl.entityUrn:
                raise ValueError("missing_dataset_urn")
            dataset_urn = mcl.entityUrn
        elif mcl.entityType == "schemaField":
            if not mcl.entityUrn:
                raise ValueError("missing_field_urn")
            key = schema_field_urn_to_key(mcl.entityUrn)
            dataset_urn = key.parent  # type: ignore[assignment]
            field_path = key.fieldPath
        else:
            raise ValueError(f"unsupported_entity_type:{mcl.entityType}")

        dataset_key = dataset_urn_to_key(dataset_urn)
        platform_urn = dataset_key.platform
        simple_platform = platform_urn.rsplit(":", 1)[-1]
        allowed = {p.lower() for p in self._config.dataset_platforms}
        if simple_platform.lower() not in allowed:
            raise ValueError("unsupported_platform")

        schema_name, table_name = self._split_dataset_name(dataset_key.name)
        tenant_id = schema_name
        new_tags = self._extract_tag_urns(mcl.aspect)
        prev_tags = self._extract_tag_urns(mcl.previousAspectValue)
        return EventContext(
            dataset_urn=dataset_urn,
            dataset_name=dataset_key.name,
            platform=simple_platform,
            schema=schema_name,
            table=table_name,
            tenant_id=tenant_id,
            field_path=field_path,
            new_tags=new_tags,
            previous_tags=prev_tags,
        )

    def _resolve_columns(
        self, dataset_urn: str, target_field: Optional[str]
    ) -> List[TokenizeColumn]:
        editable = self._graph.get_aspect(dataset_urn, EditableSchemaMetadataClass)
        if editable is None:
            return []
        columns: List[TokenizeColumn] = []
        for field in editable.editableSchemaFieldInfo or []:
            if target_field and field.fieldPath != target_field:
                continue
            pii_tags = self._collect_pii_tags(field)
            if not pii_tags:
                continue
            column_name = self._field_path_to_column(field.fieldPath)
            columns.append(
                TokenizeColumn(
                    field_path=field.fieldPath,
                    column=column_name,
                    pii_tags=pii_tags,
                )
            )
        return columns

    def _collect_pii_tags(self, field: EditableSchemaFieldInfoClass) -> List[str]:
        tags: List[str] = []
        global_tags = field.globalTags.tags if field.globalTags and field.globalTags.tags else []
        for tag in global_tags:
            urn = getattr(tag, "tag", None)
            if urn and urn.startswith(self._config.pii_tag_prefix):
                tags.append(urn)
        return tags

    @staticmethod
    def _field_path_to_column(field_path: str) -> str:
        return field_path.split(".")[-1]

    def _extract_tag_urns(
        self, aspect: Optional[Union[GenericAspectClass, GlobalTagsClass, Dict[str, Any]]]
    ) -> List[str]:
        if aspect is None:
            return []
        if isinstance(aspect, GlobalTagsClass):
            tags = aspect.tags or []
            return [tag.tag for tag in tags if getattr(tag, "tag", None)]
        if isinstance(aspect, GenericAspectClass):
            try:
                payload = json.loads(aspect.value.decode("utf-8"))
            except Exception:  # pragma: no cover - defensive fallback
                return []
        elif isinstance(aspect, dict):
            payload = aspect
        else:
            return []
        try:
            tags_obj = GlobalTagsClass.from_obj(payload)
        except Exception:  # pragma: no cover - defensive fallback
            return []
        tags = tags_obj.tags or []
        return [tag.tag for tag in tags if getattr(tag, "tag", None)]

    @staticmethod
    def _split_dataset_name(name: str) -> Tuple[str, str]:
        parts = name.split(".")
        if len(parts) < 2:
            raise ValueError("dataset_name_format")
        return parts[-2], parts[-1]

    def _build_run_id(self, dataset_name: str) -> str:
        slug = dataset_name.replace("/", "_").replace(".", "_")
        suffix = uuid.uuid4().hex[:12]
        return f"{self._config.run_id_prefix}-{slug}-{suffix}"

    def _build_external_url(self, dataset_urn: str) -> Optional[str]:
        base = self._config.external_url_base
        if not base:
            return None
        encoded = Urn.url_encode(dataset_urn)
        return f"{base.rstrip('/')}/{encoded}"

    def _make_audit_stamp(self, *, timestamp: Optional[int] = None) -> AuditStampClass:
        ts = timestamp if timestamp is not None else self._now_ms()
        return AuditStampClass(time=ts, actor=self._config.actor_urn)

    def _mcp(self, urn: str, aspect: Any) -> Any:
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        return MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)

    def _emit(self, mcp: Any) -> None:
        self._graph.emit_mcp(mcp)

    def _log(self, event: str, **data: Any) -> None:
        payload = {"event": event, **data}
        self._logger.info(json.dumps(payload, sort_keys=True))

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)


__all__ = ["TokenizeAction"]
