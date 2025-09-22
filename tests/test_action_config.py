"""Unit tests for configuration helpers in the Base64 encode action."""

from __future__ import annotations

import textwrap

import pytest

from actions.base64_action.configuration import (
    ActionConfig,
    DatabaseConfig,
    RuntimeOverrides,
    _normalize_allowlist,
)

pytestmark = pytest.mark.smoke


def test_normalize_allowlist_filters_and_lowercases_values() -> None:
    """The allowlist helper should drop empty values and lowercase entries."""
    raw_values = [" public.* ", "", None, "Sales .Orders", 123]

    assert _normalize_allowlist(raw_values) == [
        "public.*",
        "sales .orders",
        "123",
    ]


def test_database_config_from_dict_accepts_host_port_pairs() -> None:
    """Ensure DatabaseConfig handles host_port shorthand correctly."""
    config = DatabaseConfig.from_dict(
        {
            "host_port": "database.internal:15432",
            "dbname": "warehouse",
            "user": "analyst",
            "password": "secret",
        }
    )

    assert config.host == "database.internal"
    assert config.port == 15432
    assert config.dbname == "warehouse"
    assert config.user == "analyst"
    assert config.password == "secret"


def test_action_config_load_prefers_env_and_runtime_overrides(
    tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Configuration loading should honor env vars before runtime overrides."""
    config_path = tmp_path.joinpath("config.yml")
    config_path.write_text(
        textwrap.dedent(
            """
            gms_url: http://localhost:8080
            pipeline_name: postgres_local_poc
            poll_interval_seconds: 10
            page_size: 25
            database:
              host: postgres
              port: 5432
              dbname: postgres
              user: datahub
              password: datahub
            schema_allowlist:
              - public.*
            """
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://env-override:9090")
    monkeypatch.setenv("DATAHUB_PLATFORM", "postgres")
    monkeypatch.setenv("DATAHUB_PIPELINE_NAME", "env_pipeline")
    monkeypatch.setenv("DATAHUB_RUN_POLL_INTERVAL", "45")
    monkeypatch.setenv("DATAHUB_RUN_PAGE_SIZE", "200")
    monkeypatch.setenv("POSTGRES_HOST", "env-postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5439")
    monkeypatch.setenv("POSTGRES_DB", "env_db")
    monkeypatch.setenv("POSTGRES_USER", "env_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "env_pass")
    monkeypatch.setenv("DATAHUB_SCHEMA_ALLOW", "Public.Customers,Sales.* ")

    overrides = RuntimeOverrides(
        pipeline_name="runtime_pipeline",
        database_host="runtime-host",
        database_port=6000,
        database_name="runtime_db",
        database_user="runtime_user",
        database_password="runtime_pass",
        schema_allowlist=["runtime.schema"],
    )

    loaded = ActionConfig.load(path=config_path, overrides=overrides)

    # Environment variables should win over values from the YAML file.
    assert loaded.gms_url == "http://env-override:9090"
    assert loaded.poll_interval_seconds == 45
    assert loaded.page_size == 200

    # Runtime overrides should be applied after env overrides.
    assert loaded.pipeline_name == "runtime_pipeline"
    assert loaded.database.host == "runtime-host"
    assert loaded.database.port == 6000
    assert loaded.database.dbname == "runtime_db"
    assert loaded.database.user == "runtime_user"
    assert loaded.database.password == "runtime_pass"

    # Schema allowlist should reflect the runtime override and be normalized.
    assert loaded.schema_allowlist == ["runtime.schema"]
