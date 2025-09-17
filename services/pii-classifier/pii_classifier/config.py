"""Configuration helpers for the PII classifier service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schemas: List[str]
    sample_limit: int = 20

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        host = os.getenv("POSTGRES_HOST", "127.0.0.1")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        database = os.getenv("POSTGRES_DB", "sandbox")
        user = os.getenv("POSTGRES_USER", "t001")
        password = os.getenv("POSTGRES_PASSWORD", "")
        schemas_raw = os.getenv("POSTGRES_SCHEMAS", "t001")
        schemas = [schema.strip() for schema in schemas_raw.split(",") if schema.strip()]
        sample_limit = int(os.getenv("POSTGRES_SAMPLE_LIMIT", "50"))
        if not schemas:
            raise ValueError("At least one schema must be specified via POSTGRES_SCHEMAS.")
        return cls(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            schemas=schemas,
            sample_limit=sample_limit,
        )


@dataclass
class DataHubConfig:
    gms: str
    token: Optional[str]
    platform: str = "postgres"
    env: str = "PROD"
    dry_run: bool = False

    @classmethod
    def from_env(cls) -> "DataHubConfig":
        gms = os.getenv("DATAHUB_GMS", "http://localhost:8080")
        token = os.getenv("DATAHUB_TOKEN")
        platform = os.getenv("DATAHUB_PLATFORM", "postgres")
        env = os.getenv("DATAHUB_ENV", "PROD")
        dry_run = os.getenv("CLASSIFIER_DRY_RUN", "false").lower() in {"1", "true", "yes"}
        return cls(gms=gms, token=token, platform=platform, env=env, dry_run=dry_run)


@dataclass
class ClassifierConfig:
    postgres: PostgresConfig
    datahub: DataHubConfig
    rules_path: str
    min_value_samples: int = 5

    @classmethod
    def from_env(cls, rules_path: Optional[str] = None) -> "ClassifierConfig":
        pg_config = PostgresConfig.from_env()
        datahub_config = DataHubConfig.from_env()
        path = rules_path or os.getenv("CLASSIFIER_RULES_PATH", "sample/regex/rules.yml")
        min_value_samples = int(os.getenv("CLASSIFIER_MIN_VALUE_SAMPLES", "5"))
        return cls(postgres=pg_config, datahub=datahub_config, rules_path=path, min_value_samples=min_value_samples)
