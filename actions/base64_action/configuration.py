"""Configuration helpers for the Base64 encoding action."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yml")


@dataclass
class DatabaseConfig:
    host: str = "postgres"
    port: int = 5432
    dbname: str = "postgres"
    user: str = "datahub"
    password: str = "datahub"

    @classmethod
    def from_dict(cls, raw: Optional[Dict]) -> "DatabaseConfig":
        if not raw:
            return cls()
        host = raw.get("host")
        port = raw.get("port")
        host_port = raw.get("host_port")
        if host_port and (":" in host_port):
            host_candidate, port_candidate = host_port.split(":", 1)
            host = host or host_candidate
            port = port or port_candidate
        return cls(
            host=host or cls.host,
            port=int(port or cls.port),
            dbname=raw.get("dbname", cls.dbname),
            user=raw.get("user", cls.user),
            password=raw.get("password", cls.password),
        )

    def apply_overrides(self, overrides: "RuntimeOverrides") -> None:
        if overrides.database_host:
            self.host = overrides.database_host
        if overrides.database_port:
            self.port = overrides.database_port
        if overrides.database_name:
            self.dbname = overrides.database_name
        if overrides.database_user:
            self.user = overrides.database_user
        if overrides.database_password:
            self.password = overrides.database_password


@dataclass
class RuntimeOverrides:
    gms_url: Optional[str] = None
    platform: Optional[str] = None
    pipeline_name: Optional[str] = None
    database_host: Optional[str] = None
    database_port: Optional[int] = None
    database_name: Optional[str] = None
    database_user: Optional[str] = None
    database_password: Optional[str] = None
    schema_allowlist: Optional[Sequence[str]] = None


@dataclass
class ActionConfig:
    gms_url: str = "http://localhost:8080"
    platform: str = "postgres"
    pipeline_name: Optional[str] = "postgres_local_poc"
    poll_interval_seconds: int = 15
    page_size: int = 100
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    schema_allowlist: List[str] = field(default_factory=list)

    def apply_overrides(self, overrides: Optional[RuntimeOverrides]) -> None:
        if not overrides:
            return
        if overrides.gms_url:
            self.gms_url = overrides.gms_url
        if overrides.platform:
            self.platform = overrides.platform
        if overrides.pipeline_name:
            self.pipeline_name = overrides.pipeline_name
        self.database.apply_overrides(overrides)
        if overrides.schema_allowlist is not None:
            self.schema_allowlist = _normalize_allowlist(overrides.schema_allowlist)

    @classmethod
    def load(
        cls, path: Path = DEFAULT_CONFIG_PATH, overrides: Optional[RuntimeOverrides] = None
    ) -> "ActionConfig":
        config = cls()
        raw: Dict = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                raw_data = yaml.safe_load(handle) or {}
                if isinstance(raw_data, dict):
                    raw = raw_data
        if raw:
            config.gms_url = raw.get("gms_url", config.gms_url)
            config.platform = raw.get("platform", config.platform)
            config.pipeline_name = raw.get("pipeline_name", config.pipeline_name)
            config.poll_interval_seconds = int(
                raw.get("poll_interval_seconds", config.poll_interval_seconds)
            )
            config.page_size = int(raw.get("page_size", config.page_size))
            config.database = DatabaseConfig.from_dict(raw.get("database"))
            schema_raw: Optional[Iterable[Any]]
            schema_raw = raw.get("schema_allowlist")
            if not schema_raw:
                pattern_section = raw.get("schema_pattern")
                if isinstance(pattern_section, dict):
                    schema_raw = pattern_section.get("allow")
                else:
                    schema_raw = pattern_section
            config.schema_allowlist = _normalize_allowlist(schema_raw)

        # Environment overrides take precedence.
        config.gms_url = os.environ.get("DATAHUB_GMS_URL", config.gms_url)
        config.platform = os.environ.get("DATAHUB_PLATFORM", config.platform)
        config.pipeline_name = os.environ.get("DATAHUB_PIPELINE_NAME", config.pipeline_name)
        config.poll_interval_seconds = int(
            os.environ.get("DATAHUB_RUN_POLL_INTERVAL", config.poll_interval_seconds)
        )
        config.page_size = int(os.environ.get("DATAHUB_RUN_PAGE_SIZE", config.page_size))
        allow_env = os.environ.get("DATAHUB_SCHEMA_ALLOW") or os.environ.get(
            "BASE64_SCHEMA_ALLOW"
        )
        if allow_env:
            config.schema_allowlist = _normalize_allowlist(allow_env.split(","))
        config.database = DatabaseConfig(
            host=os.environ.get("POSTGRES_HOST", config.database.host),
            port=int(os.environ.get("POSTGRES_PORT", config.database.port)),
            dbname=os.environ.get("POSTGRES_DB", config.database.dbname),
            user=os.environ.get("POSTGRES_USER", config.database.user),
            password=os.environ.get("POSTGRES_PASSWORD", config.database.password),
        )
        overrides = overrides or RuntimeOverrides()
        config.apply_overrides(overrides)
        return config


@dataclass
class DatasetIdentifier:
    urn: str
    database: str
    schema: str
    table: str

    @property
    def schema_table(self) -> str:
        return f"{self.schema}.{self.table}"


def _normalize_allowlist(raw_values: Optional[Iterable[Any]]) -> List[str]:
    if not raw_values:
        return []
    normalized: List[str] = []
    for value in raw_values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized.append(text.lower())
    return normalized


__all__ = [
    "ActionConfig",
    "DatabaseConfig",
    "DatasetIdentifier",
    "RuntimeOverrides",
    "DEFAULT_CONFIG_PATH",
    "_normalize_allowlist",
]
