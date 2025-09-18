"""Pydantic models describing the tokenize action configuration."""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field, SecretStr

from datahub.configuration.common import ConfigModel


class DataFlowSettings(BaseModel):
    orchestrator: str = Field(
        "actions-tokenize",
        description="Logical orchestrator or platform name for the DataFlow URN.",
    )
    flow_id: str = Field(
        "tokenize",
        description="Identifier portion of the DataFlow URN.",
    )
    env: str = Field(
        "DEV",
        description="Environment suffix used in the DataFlow URN (eg. DEV, PROD).",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Optional platform instance qualifier for the DataFlow URN.",
    )
    name: Optional[str] = Field(
        default=None,
        description="Display name for the DataFlow entity.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Free-form description for the DataFlow entity.",
    )
    external_url: Optional[str] = Field(
        default=None,
        description="External URL to surface on the DataFlow entity (eg. runbook).",
    )
    custom_properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional custom properties for the DataFlow entity.",
    )


class DataJobSettings(BaseModel):
    job_id: str = Field(
        "tokenize-action",
        description="Identifier portion of the DataJob within the parent DataFlow.",
    )
    name: Optional[str] = Field(
        default=None,
        description="Display name for the DataJob entity.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Free-form description for the DataJob entity.",
    )
    external_url: Optional[str] = Field(
        default=None,
        description="External URL to surface on the DataJob entity.",
    )
    custom_properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional custom properties for the DataJob entity.",
    )


class TenantCredential(BaseModel):
    tenant_id: str = Field(
        ..., description="Tenant identifier resolved from dataset schema or metadata."
    )
    username: Optional[str] = Field(
        default=None,
        description=(
            "Database username to authenticate as. Defaults to the tenant identifier "
            "when omitted."
        ),
    )
    password: SecretStr = Field(
        ..., description="Password for the tenant-scoped database credential."
    )
    role: Optional[str] = Field(
        default=None,
        description="Optional database role to set after connecting (eg. SET ROLE).",
    )
    search_path: Optional[str] = Field(
        default=None,
        description="Optional comma-delimited search_path override per tenant.",
    )


class PostgresSettings(BaseModel):
    host: str = Field(..., description="Database hostname or service name.")
    port: int = Field(5432, description="Database port.")
    database: str = Field(..., description="Database name to connect to.")
    connect_timeout_seconds: int = Field(
        10, description="Connection timeout in seconds."
    )
    sslmode: Optional[str] = Field(
        default=None,
        description=(
            "Optional libpq sslmode (disable, require, verify-full, etc). "
            "Omitted falls back to driver default."
        ),
    )
    application_name: str = Field(
        "datahub-actions-tokenize",
        description="application_name reported to Postgres.",
    )
    tenants: List[TenantCredential] = Field(
        default_factory=list,
        description="Collection of tenant credential entries keyed by tenant identifier.",
    )
    default_search_path: Optional[str] = Field(
        default=None,
        description="Fallback search_path when tenant entry omits one.",
    )


class TokenizeActionConfig(ConfigModel):
    token_tag: str = Field(
        "urn:li:tag:tokenize-now",
        description="Tag URN that triggers tokenization when applied to a dataset or field.",
    )
    pii_tag_prefix: str = Field(
        "urn:li:tag:pii-",
        description="Tag URN prefix identifying columns eligible for tokenization.",
    )
    dataset_platforms: List[str] = Field(
        default_factory=lambda: ["postgres"],
        description="Data platform identifiers (from dataset URNs) that this action will process.",
    )
    regex_token_pattern: str = Field(
        r"^tok_.+_poc$",
        description="Regular expression used to detect already-tokenized values.",
    )
    use_base64_tokens: bool = Field(
        True,
        description=(
            "When true, use reversible base64 encoding inside the token body. "
            "Otherwise, a SHA-256 hex digest is generated."
        ),
    )
    dry_run: bool = Field(
        False,
        description="When enabled, skip database writes while still emitting run metadata.",
    )
    run_id_prefix: str = Field(
        "tokenize",
        description="Prefix for DataProcessInstance identifiers emitted per run.",
    )
    actor_urn: str = Field(
        "urn:li:corpuser:tokenize-action",
        description="CorpUser URN recorded as the actor for emitted run metadata.",
    )
    external_url_base: Optional[str] = Field(
        default=None,
        description=(
            "Optional base URL for constructing run externalUrl links. "
            "When provided, dataset URNs are appended to build UI hyperlinks."
        ),
    )
    dataflow: DataFlowSettings = Field(
        default_factory=DataFlowSettings,
        description="Metadata settings for the DataFlow that anchors run lineage.",
    )
    datajob: DataJobSettings = Field(
        default_factory=DataJobSettings,
        description="Metadata settings for the DataJob that anchors run lineage.",
    )
    postgres: PostgresSettings = Field(
        ..., description="Postgres connectivity and credential configuration."
    )
    max_columns: Optional[int] = Field(
        default=None,
        description="Optional safety limit on the number of columns tokenized per run.",
    )
    runtime_custom_properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional static custom properties to inject into run metadata.",
    )

    class Config:
        arbitrary_types_allowed = True
