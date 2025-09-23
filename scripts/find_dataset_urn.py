#!/usr/bin/env python3
"""Locate a dataset URN in DataHub by name and platform."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

import requests

DEFAULT_PLATFORM = "postgres"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Find a dataset URN via DataHub GraphQL")
    parser.add_argument("name", help="Dataset name (e.g. public.customers)")
    parser.add_argument(
        "--platform",
        default=DEFAULT_PLATFORM,
        help="Platform name, default %(default)s",
    )
    return parser


def execute_graphql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    gms = os.environ.get("DATAHUB_GMS")
    token = os.environ.get("DATAHUB_TOKEN")
    if not gms:
        raise SystemExit("DATAHUB_GMS must be set")
    payload = {"query": query, "variables": variables}
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = requests.post(
        f"{gms.rstrip('/')}/graphql",
        headers=headers,
        data=json.dumps(payload),
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("errors"):
        raise SystemExit(json.dumps(data["errors"], indent=2))
    return data["data"]


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    query = """
    query searchDatasets($input: SearchAcrossEntitiesInput!) {
      searchAcrossEntities(input: $input) {
        searchResults {
          entity {
            urn
            ... on Dataset {
              platform { urn }
              name
            }
          }
        }
      }
    }
    """
    filters = [
        {"field": "platform", "values": [f"urn:li:dataPlatform:{args.platform}"]}
    ]
    variables = {
        "input": {
            "types": ["DATASET"],
            "query": args.name,
            "filters": filters,
            "start": 0,
            "count": 10,
        }
    }
    data = execute_graphql(query, variables)
    results = data.get("searchAcrossEntities", {}).get("searchResults", [])
    if not results:
        raise SystemExit("No datasets matched the criteria")
    urn = results[0]["entity"]["urn"]
    print(urn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
