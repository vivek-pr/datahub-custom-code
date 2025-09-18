import pytest

from tools.verify_poc import (
    VerificationError,
    ensure_unique_tags,
    evaluate_run_transitions,
    extract_schema_field_tags,
    parse_postgres_dataset_urn,
    sql_literal,
)


def test_extract_schema_field_tags_and_deduplication():
    payload = {
        "editableSchemaMetadata": {
            "editableSchemaFieldInfo": [
                {
                    "fieldPath": "customers.email",
                    "globalTags": {
                        "tags": [
                            {"tag": "urn:li:tag:pii-email"},
                            {"tag": "urn:li:tag:pii-email"},
                            {"tag": "urn:li:tag:internal"},
                        ]
                    },
                },
                {
                    "fieldPath": "customers.pan",
                    "globalTags": {
                        "tags": [
                            {"tag": "urn:li:tag:pii-pan"},
                            {"tag": "urn:li:tag:pii-pan"},
                            {"tag": "urn:li:tag:tokenize-now"},
                        ]
                    },
                },
            ]
        }
    }
    extracted = extract_schema_field_tags(payload)
    assert extracted["customers.email"] == [
        "urn:li:tag:pii-email",
        "urn:li:tag:pii-email",
        "urn:li:tag:internal",
    ]
    deduped = ensure_unique_tags(extracted)
    assert deduped["customers.email"] == [
        "urn:li:tag:pii-email",
        "urn:li:tag:internal",
    ]
    assert deduped["customers.pan"] == [
        "urn:li:tag:pii-pan",
        "urn:li:tag:tokenize-now",
    ]


def test_evaluate_run_transitions_success():
    states = ["RUNNING", "RUNNING", "COMPLETED"]
    result = evaluate_run_transitions(states)
    assert result["final"] == "COMPLETED"
    assert result["states"] == ["RUNNING", "RUNNING", "COMPLETED"]


def test_evaluate_run_transitions_failure_on_invalid_start():
    with pytest.raises(VerificationError):
        evaluate_run_transitions(["FAILED"], expect_success=True)


def test_evaluate_run_transitions_negative_expectation():
    states = ["RUNNING", "FAILED"]
    result = evaluate_run_transitions(states, expect_success=False)
    assert result["final"] == "FAILED"


def test_parse_postgres_dataset_urn_happy_path():
    database, schema, table = parse_postgres_dataset_urn(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,sandbox.t001.customers,PROD)"
    )
    assert database == "sandbox"
    assert schema == "t001"
    assert table == "customers"


def test_parse_postgres_dataset_urn_invalid():
    with pytest.raises(VerificationError):
        parse_postgres_dataset_urn("urn:li:dataset:invalid")


def test_sql_literal_handles_common_types():
    assert sql_literal("O'Hare") == "'O''Hare'"
    assert sql_literal(10) == "10"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(None) == "NULL"
