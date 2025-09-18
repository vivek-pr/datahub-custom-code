import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

sys.path.append(
    str(Path(__file__).resolve().parents[2] / "services" / "actions-tokenize")
)

from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GenericAspectClass,
    GlobalTagsClass,
    MetadataChangeLogClass,
    TagAssociationClass,
)

from actions_tokenize.action import TokenizeAction
from actions_tokenize.config import (
    PostgresSettings,
    TenantCredential,
    TokenizeActionConfig,
)
from actions_tokenize.postgres import (
    ColumnTokenizationPlan,
    TokenizeColumn,
    TokenizationOutcome,
)


class StubGraph:
    def __init__(self, aspects):
        self._aspects = aspects
        self.emitted = []

    def get_aspect(self, urn, aspect_class):
        return self._aspects.get((urn, aspect_class))

    def emit_mcp(self, proposal):
        self.emitted.append(proposal)


class StubPostgres:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    def tokenize(
        self,
        *,
        tenant_id,
        schema,
        table,
        columns,
        token_pattern,
        use_base64,
        dry_run,
    ):
        self.calls.append(
            {
                "tenant_id": tenant_id,
                "schema": schema,
                "table": table,
                "columns": columns,
                "token_pattern": token_pattern,
                "use_base64": use_base64,
                "dry_run": dry_run,
            }
        )
        return self.outcome


def make_tags_aspect(tags):
    payload = GlobalTagsClass(
        tags=[TagAssociationClass(tag=tag) for tag in tags]
    ).to_obj()
    return GenericAspectClass(
        value=json.dumps(payload).encode("utf-8"),
        contentType="application/json",
    )


@pytest.fixture
def base_config():
    return TokenizeActionConfig(
        postgres=PostgresSettings(
            host="localhost",
            database="sandbox",
            tenants=[
                TenantCredential(tenant_id="t001", password=SecretStr("password")),
            ],
        ),
    )


@pytest.fixture
def dataset_urn():
    return "urn:li:dataset:(urn:li:dataPlatform:postgres,sandbox.t001.customers,PROD)"


@pytest.fixture
def editable_schema(dataset_urn):
    field = EditableSchemaFieldInfoClass(
        fieldPath="first_name",
        globalTags=GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:pii-name")]
        ),
    )
    return EditableSchemaMetadataClass(editableSchemaFieldInfo=[field])


def build_action(config, graph):
    ctx = SimpleNamespace(graph=graph)
    action = TokenizeAction(config, ctx)
    return action


def test_resolve_columns_filters_to_pii_fields(base_config, dataset_urn, editable_schema):
    graph = StubGraph({(dataset_urn, EditableSchemaMetadataClass): editable_schema})
    action = build_action(base_config, graph)

    columns = action._resolve_columns(dataset_urn, target_field=None)
    assert len(columns) == 1
    assert columns[0].column == "first_name"


def test_act_runs_tokenization_for_dataset_event(base_config, dataset_urn, editable_schema):
    graph = StubGraph({(dataset_urn, EditableSchemaMetadataClass): editable_schema})
    action = build_action(base_config, graph)

    tokenize_column = TokenizeColumn(
        field_path="first_name",
        column="first_name",
        pii_tags=["urn:li:tag:pii-name"],
    )
    outcome = TokenizationOutcome(
        total_rows=2,
        per_column=[
            ColumnTokenizationPlan(
                column=tokenize_column,
                rows_to_update=2,
                data_type="text",
            )
        ],
    )
    stub_pg = StubPostgres(outcome)
    action._postgres = stub_pg

    envelope = SimpleNamespace(
        event_type=SimpleNamespace(value="METADATA_CHANGE_LOG_EVENT"),
        event=MetadataChangeLogClass(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspectName="globalTags",
            aspect=make_tags_aspect(
                ["urn:li:tag:tokenize-now", "urn:li:tag:pii-name"]
            ),
            previousAspectValue=make_tags_aspect(["urn:li:tag:pii-name"]),
        ),
    )

    action.act(envelope)

    assert stub_pg.calls
    call = stub_pg.calls[0]
    assert call["tenant_id"] == "t001"
    assert call["schema"] == "t001"
    assert call["table"] == "customers"
    assert call["columns"][0].field_path == "first_name"
    assert graph.emitted, "Expected run metadata to be emitted"
