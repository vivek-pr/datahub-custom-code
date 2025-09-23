"""Shared data models for the tokenization action."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Set

from pydantic import BaseModel, Field, validator

DATASET_URN_RE = r"^urn:li:dataset:\((?P<platform>urn:li:dataPlatform:[^,]+),(?P<name>[^,]+),(?P<env>[^)]+)\)$"


class DatasetRef(BaseModel):
    """Parsed view of a DataHub dataset URN."""

    urn: str
    platform: str
    database: str
    schema_name: str = Field(alias="schema")
    table: str
    env: str

    class Config:
        allow_population_by_field_name = True

    @validator("urn")
    def _validate_urn(cls, value: str) -> str:  # noqa: N805
        from re import match

        if not match(DATASET_URN_RE, value):
            raise ValueError(f"Unsupported dataset URN: {value}")
        return value

    @classmethod
    def from_urn(cls, urn: str) -> "DatasetRef":
        import re

        match = re.match(DATASET_URN_RE, urn)
        if not match:
            raise ValueError(f"Unsupported dataset URN: {urn}")
        platform_urn = match.group("platform")
        name = match.group("name")
        env = match.group("env")
        parts = name.split(".")
        if len(parts) == 2:
            database = "default"
            schema_name, table = parts
        else:
            database = parts[0]
            schema_name = parts[1]
            table = ".".join(parts[2:]) if len(parts) > 2 else parts[1]
        platform = platform_urn.split(":")[-1]
        return cls(
            urn=urn,
            platform=platform,
            database=database,
            schema_name=schema_name,
            table=table,
            env=env,
        )

    @property
    def schema(self) -> str:
        return self.schema_name

    @property
    def table_expression(self) -> str:
        if self.platform == "databricks":
            return f"`{self.database}`.`{self.schema_name}`.`{self.table}`"
        return f"{self.schema_name}.{self.table}"


class FieldMetadata(BaseModel):
    field_path: str
    native_data_type: Optional[str] = None
    tags: Set[str] = Field(default_factory=set)

    @property
    def column(self) -> str:
        return self.field_path.split(".")[-1]


class DatasetMetadata(BaseModel):
    urn: str
    ref: DatasetRef
    name: Optional[str] = None
    platform: str
    global_tags: Set[str] = Field(default_factory=set)
    fields: List[FieldMetadata] = Field(default_factory=list)
    editable_properties: Dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_graphql(cls, urn: str, payload: Dict[str, object]) -> "DatasetMetadata":
        ref = DatasetRef.from_urn(urn)
        global_tags = _extract_tag_set(payload.get("globalTags"))
        editable_props = payload.get("editableProperties", {}) or {}
        custom_props = (
            editable_props.get("customProperties", {})
            if isinstance(editable_props, dict)
            else {}
        )
        schema_metadata = payload.get("schemaMetadata") or {}
        editable_schema = payload.get("editableSchemaMetadata") or {}
        editable_field_info = {
            info.get("fieldPath"): _extract_tag_set(info.get("globalTags"))
            for info in editable_schema.get("editableSchemaFieldInfo", []) or []
        }
        fields: List[FieldMetadata] = []
        for field in schema_metadata.get("fields", []) or []:
            field_path = field.get("fieldPath")
            if not field_path:
                continue
            tags = _extract_tag_set(field.get("globalTags"))
            if field_path in editable_field_info:
                tags = tags.union(editable_field_info[field_path])
            fields.append(
                FieldMetadata(
                    field_path=field_path,
                    native_data_type=field.get("nativeDataType"),
                    tags=tags,
                )
            )
        return cls(
            urn=urn,
            ref=ref,
            name=payload.get("name"),
            platform=ref.platform,
            global_tags=global_tags,
            fields=fields,
            editable_properties=dict(custom_props),
        )

    def field_map(self) -> Dict[str, FieldMetadata]:
        """Return a mapping keyed by column name for convenience."""

        mapping: Dict[str, FieldMetadata] = {}
        for field in self.fields:
            mapping.setdefault(field.column, field)
        return mapping


def _extract_tag_set(tag_payload: Optional[object]) -> Set[str]:
    tags: Set[str] = set()
    if not tag_payload:
        return tags
    tags_list = []
    if isinstance(tag_payload, dict):
        tags_list = tag_payload.get("tags") or []
    for tag_entry in tags_list:
        if not isinstance(tag_entry, dict):
            continue
        tag = tag_entry.get("tag")
        if isinstance(tag, dict):
            urn = tag.get("urn")
        else:
            urn = tag_entry.get("urn")
        if urn:
            tags.add(urn)
    return tags


@dataclass
class TokenizationResult:
    columns: Sequence[str]
    rows_updated: int
    rows_skipped: int


class RunStatus(BaseModel):
    run_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    platform: str
    columns: List[str]
    rows_updated: int
    rows_skipped: int
    status: str
    message: str

    class Config:
        orm_mode = True


class HealthResponse(BaseModel):
    status: str = "ok"
