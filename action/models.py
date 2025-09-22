"""Pydantic models shared across the action."""

from __future__ import annotations

import re
from typing import List

from pydantic import BaseModel, Field, validator

_DATASET_RE = re.compile(
    r"^urn:li:dataset:\((?P<platform>urn:li:dataPlatform:[^,]+),(?P<name>[^,]+),(?P<env>[^)]+)\)$"
)


class DatasetRef(BaseModel):
    platform: str
    database: str
    schema_name: str = Field(alias="schema")
    table: str
    env: str

    class Config:
        allow_population_by_field_name = True

    @classmethod
    def from_urn(cls, urn: str) -> "DatasetRef":
        match = _DATASET_RE.match(urn)
        if not match:
            raise ValueError(f"Unsupported dataset URN: {urn}")
        platform_urn = match.group("platform")
        name = match.group("name")
        env = match.group("env")
        parts = name.split(".")
        if len(parts) < 2:
            raise ValueError(f"Dataset URN must include schema and table: {urn}")
        if len(parts) == 2:
            database = "default"
            schema_name, table = parts
        else:
            database, schema_name, table = parts[0], parts[1], ".".join(parts[2:])
        platform = platform_urn.split(":")[-1]
        return cls(
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


class TriggerRequest(BaseModel):
    dataset: str
    columns: List[str] = Field(..., min_items=1)
    limit: int = Field(100, gt=0, le=1000)

    @validator("columns")
    def _strip_columns(cls, value: List[str]) -> List[str]:
        cleaned = [column.strip() for column in value if column.strip()]
        if not cleaned:
            raise ValueError("columns must not be empty")
        return cleaned

    @property
    def dataset_ref(self) -> DatasetRef:
        return DatasetRef.from_urn(self.dataset)


class TriggerResponse(BaseModel):
    updated_count: int
    skipped_count: int
    platform: str
    elapsed_ms: float


class HealthResponse(BaseModel):
    status: str = "ok"
