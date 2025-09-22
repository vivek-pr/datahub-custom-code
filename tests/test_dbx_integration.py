import os

import pytest

from action import db_dbx
from action.models import DatasetRef
from action.sdk_adapter import TokenizationSDKAdapter


def test_parse_jdbc_url_basic():
    url = (
        "jdbc:databricks://example.cloud.databricks.com?"
        "httpPath=/sql/1.0/warehouses/abc&AuthMech=3&UID=token&PWD=secrettoken"
    )
    parsed = db_dbx.parse_jdbc_url(url)
    assert parsed["host"] == "example.cloud.databricks.com"
    assert parsed["http_path"] == "/sql/1.0/warehouses/abc"
    assert parsed["token"] == "secrettoken"


@pytest.mark.skipif(
    not os.environ.get("DBX_TEST_JDBC_URL"),
    reason="Databricks test warehouse not configured",
)
def test_databricks_tokenization_round_trip(monkeypatch):
    jdbc_url = os.environ["DBX_TEST_JDBC_URL"]
    dataset = DatasetRef.from_urn(
        "urn:li:dataset:(urn:li:dataPlatform:databricks,tokenize.schema.customers,PROD)"
    )
    adapter = TokenizationSDKAdapter(mode="dummy")

    result = db_dbx.tokenize_table(jdbc_url, dataset, ["email", "phone"], 100, adapter)
    assert "updated_count" in result
    assert "skipped_count" in result
