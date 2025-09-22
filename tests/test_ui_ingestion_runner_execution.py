import json
from typing import Any, Dict

import pytest

from scripts import ui_ingestion_runner as runner


class _DummyClient:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self.payload = payload

    def query(
        self,
        query: str,
        variables: Dict[str, Any],
        operation_name: str | None = None,
    ) -> Dict[str, Any]:
        return self.payload


def test_list_pending_requests_accepts_id_and_pending_status() -> None:
    response = {
        "ingestionSource": {
            "executions": {
                "executionRequests": [
                    {"id": "urn:li:dataHubExecutionRequest:123", "result": {"status": "PENDING"}},
                    {"id": "urn:li:dataHubExecutionRequest:124", "result": {"status": "SUCCEEDED"}},
                    {"urn": "urn:li:dataHubExecutionRequest:125", "result": None},
                ]
            }
        }
    }
    client = _DummyClient(response)
    pending = runner.list_pending_requests(client, "urn:li:datahub:source")
    assert "urn:li:dataHubExecutionRequest:123" in pending
    assert "urn:li:dataHubExecutionRequest:125" in pending
    assert "urn:li:dataHubExecutionRequest:124" not in pending


@pytest.mark.parametrize(
    "raw,expected_host,expected_schema,expected_columns",
    [
        (
            json.dumps(
                {
                    "database": {
                        "host": "analytics",
                        "port": 5433,
                        "name": "warehouse",
                        "user": "encoder",
                        "password": "s3cret",
                    },
                    "schemaAllow": ["public.*", "sales.*"],
                    "columnAllow": {"public.customers": ["email", "phone"]},
                }
            ),
            "analytics",
            ["public.*", "sales.*"],
            {("public", "customers"): {"email", "phone"}},
        ),
        (
            {
                "db": {"host": "postgres", "port": "5432", "database": "postgres"},
                "columns": [
                    {"schema": "public", "table": "orders", "columns": ["notes"]},
                ],
            },
            "postgres",
            [],
            {("public", "orders"): {"notes"}},
        ),
    ],
)
def test_tokenization_overrides_parsing(
    raw: Any,
    expected_host: str,
    expected_schema: list[str],
    expected_columns: Dict[tuple[str, str], set[str]],
) -> None:
    overrides = runner.TokenizationOverrides.from_argument(raw)
    assert overrides.database_host == expected_host
    assert overrides.schema_allowlist == expected_schema
    assert overrides.column_allowlist == expected_columns
