#!/usr/bin/env python3
"""Minimal ingestion executor for UI-triggered runs.

Polls DataHub GraphQL for execution requests that do not yet have results and
replays them using the standard `datahub ingest run` CLI. Meant for local PoC
use only.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Dict, Iterable, List, Optional, Sequence

import requests
import yaml

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080").rstrip("/")
POLL_SECONDS = int(os.environ.get("UI_RUNNER_POLL_INTERVAL", "15"))
PAGE_SIZE = int(os.environ.get("UI_RUNNER_PAGE_SIZE", "10"))
DEFAULT_SINK_SERVER = os.environ.get("UI_RUNNER_SINK_SERVER", f"{GMS_URL}")
DEFAULT_EXECUTOR_ID = os.environ.get("UI_RUNNER_EXECUTOR_ID", "ui-ingestion-runner")
DEBUG = os.environ.get("UI_RUNNER_DEBUG", "0") == "1"


class GraphQLClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    def query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        response = requests.post(
            self.endpoint,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        if "errors" in body:
            raise RuntimeError(f"GraphQL error: {body['errors']}")
        return body.get("data", {})


def list_ingestion_sources(client: GraphQLClient) -> Sequence[Dict]:
    query = """
    query ListSources($start: Int!, $count: Int!) {
      listIngestionSources(input: {start: $start, count: $count}) {
        total
        ingestionSources {
          urn
          name
          type
        }
      }
    }
    """
    sources: List[Dict] = []
    start = 0
    while True:
        data = client.query(query, {"start": start, "count": PAGE_SIZE})
        wrapper = data.get("listIngestionSources") or {}
        page = wrapper.get("ingestionSources") or []
        sources.extend(page)
        if len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return sources


def list_pending_requests(client: GraphQLClient, source_urn: str) -> List[str]:
    query = """
    query PendingExecutions($urn: String!, $start: Int!, $count: Int!) {
      ingestionSource(urn: $urn) {
        executions(start: $start, count: $count) {
          executionRequests {
            urn
            result { status }
          }
        }
      }
    }
    """
    pending: List[str] = []
    data = client.query(query, {"urn": source_urn, "start": 0, "count": PAGE_SIZE})
    executions = (
        data.get("ingestionSource", {})
        .get("executions", {})
        .get("executionRequests", [])
    )
    for execution in executions:
        if not execution:
            continue
        urn = execution.get("urn")
        result = execution.get("result")
        # A null result means RUNNING/PENDING. We only handle runs with no result yet.
        if urn and (not result or not result.get("status")):
            pending.append(urn)
    return pending


def load_execution_recipe(
    client: GraphQLClient, execution_urn: str
) -> tuple[Dict, Optional[str]]:
    query = """
    query GetExecution($urn: String!) {
      executionRequest(urn: $urn) {
        urn
        requestedExecutorId
        input {
          arguments {
            key
            value
          }
        }
      }
    }
    """
    data = client.query(query, {"urn": execution_urn})
    execution = data.get("executionRequest") or {}
    arguments: Iterable[Dict] = execution.get("input", {}).get("arguments", [])
    arg_map = {arg.get("key"): arg.get("value") for arg in arguments if arg}
    raw_recipe = arg_map.get("recipe")
    if not raw_recipe:
        raise RuntimeError(f"Execution {execution_urn} missing recipe argument")
    recipe_dict = json.loads(raw_recipe)
    # Ensure a sink is present so CLI emits to our local GMS.
    recipe_dict.setdefault(
        "sink",
        {
            "type": "datahub-rest",
            "config": {
                "server": DEFAULT_SINK_SERVER,
            },
        },
    )
    if DEBUG:
        print(
            f"Loaded recipe for {execution_urn}: {json.dumps(recipe_dict, indent=2)}",
            file=sys.stderr,
        )
    return recipe_dict, execution.get("requestedExecutorId")


def run_ingestion(recipe: Dict, execution_urn: str, executor_id: str) -> int:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as temp_recipe:
        yaml.safe_dump(recipe, temp_recipe)
        temp_path = temp_recipe.name
    try:
        print(
            f"[ui-runner] Running ingestion recipe {temp_path}"
            f" for execution {execution_urn} (executor={executor_id})",
            flush=True,
        )
        command = [
            "datahub",
            "ingest",
            "run",
            "--executor-id",
            executor_id,
            "--execution-request-urn",
            execution_urn,
            "-c",
            temp_path,
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.stdout:
            print(completed.stdout, end="", flush=True)
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr, flush=True)
        return completed.returncode
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def main() -> int:
    client = GraphQLClient(f"{GMS_URL}/api/graphql")
    handled: set[str] = set()
    print(
        f"[ui-runner] Starting poller against {GMS_URL} (interval={POLL_SECONDS}s)",
        flush=True,
    )
    while True:
        try:
            for source in list_ingestion_sources(client):
                source_urn = source.get("urn")
                if not source_urn:
                    continue
                for execution_urn in list_pending_requests(client, source_urn):
                    if execution_urn in handled:
                        continue
                    print(
                        f"[ui-runner] Detected pending execution {execution_urn}"
                        f" for source {source.get('name', source_urn)}",
                        flush=True,
                    )
                    recipe, requested_executor = load_execution_recipe(client, execution_urn)
                    executor_id = requested_executor or DEFAULT_EXECUTOR_ID
                    exit_code = run_ingestion(recipe, execution_urn, executor_id)
                    status = "SUCCEEDED" if exit_code == 0 else f"FAILED({exit_code})"
                    print(
                        f"[ui-runner] Execution {execution_urn} finished with status {status}",
                        flush=True,
                    )
                    handled.add(execution_urn)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[ui-runner] Error while processing executions: {exc}", file=sys.stderr)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
