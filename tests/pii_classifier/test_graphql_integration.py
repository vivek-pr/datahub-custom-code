from __future__ import annotations

import json
import os

import pytest
import requests


@pytest.mark.integration
def test_dataset_fields_have_pii_tags():
    gms = os.getenv("DATAHUB_GMS")
    dataset_urn = os.getenv("CLASSIFIER_TEST_DATASET_URN")
    if not gms or not dataset_urn:
        pytest.skip("DATAHUB_GMS and CLASSIFIER_TEST_DATASET_URN required for integration test")

    headers = {"Content-Type": "application/json"}
    token = os.getenv("DATAHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    query = {
        "query": """
        query Dataset($urn: String!) {
          dataset(urn: $urn) {
            schema {
              schemaMetadata {
                fields {
                  fieldPath
                  globalTags {
                    tags {
                      tag {
                        urn
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """,
        "variables": {"urn": dataset_urn},
    }

    response = requests.post(f"{gms}/api/graphql", headers=headers, data=json.dumps(query), timeout=30)
    response.raise_for_status()
    data = response.json()
    fields = data["data"]["dataset"]["schema"]["schemaMetadata"]["fields"]
    assert any(
        tag_entry["tag"]["urn"].startswith("urn:li:tag:pii-")
        for field in fields
        for tag_entry in (field.get("globalTags") or {}).get("tags", [])
    ), "Expected at least one field to contain a pii-* tag"
