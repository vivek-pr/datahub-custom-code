"""Minimal ingestion executor for UI-triggered runs.

Polls DataHub GraphQL for execution requests and replays them using the
DataHub Python pipeline so that run status and logs are published back to GMS.
After a successful ingestion the Base64 tokenization action is triggered.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg
import requests

from datahub.ingestion.run.pipeline import Pipeline

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from actions.base64_action.action import (  # noqa: E402
    ActionConfig,
    Base64EncodeAction,
    RuntimeOverrides,
)

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080").rstrip("/")
POLL_SECONDS = int(os.environ.get("UI_RUNNER_POLL_INTERVAL", "15"))
PAGE_SIZE = int(os.environ.get("UI_RUNNER_PAGE_SIZE", "10"))
DEFAULT_SINK_SERVER = os.environ.get("UI_RUNNER_SINK_SERVER", f"{GMS_URL}")
DEFAULT_EXECUTOR_ID = os.environ.get("UI_RUNNER_EXECUTOR_ID", "ui-ingestion-runner")
DEBUG = os.environ.get("UI_RUNNER_DEBUG", "0") == "1"
LOG_LEVEL = os.environ.get("UI_RUNNER_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [ui-runner] %(message)s",
)
LOGGER = logging.getLogger("ui-ingestion-runner")


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
        LOGGER.debug(
            "Loaded recipe for %s: %s",
            execution_urn,
            json.dumps(recipe_dict, indent=2),
        )
    return recipe_dict, execution.get("requestedExecutorId")


def sanitize_host(recipe: Dict) -> Optional[Tuple[str, str]]:
    source_config = recipe.get("source", {}).get("config", {})
    host_port = source_config.get("host_port")
    if not host_port:
        return None
    host, sep, port = host_port.partition(":")
    if not sep:
        port = ""
    fallback_host = os.environ.get("UI_RUNNER_DEFAULT_DB_HOST", "postgres")
    normalized = host.strip().lower()
    if normalized in {"localhost", "127.0.0.1", "0.0.0.0", "host.docker.internal"}:
        new_host = fallback_host
    else:
        new_host = host
    new_port = port or os.environ.get("UI_RUNNER_DEFAULT_DB_PORT", "5432")
    rewritten = f"{new_host}:{new_port}"
    if rewritten != host_port:
        source_config["host_port"] = rewritten
        LOGGER.info(
            "Rewriting source host_port from %s to %s for container accessibility",
            host_port,
            rewritten,
        )
        return host_port, rewritten
    return None


def extract_schema_allowlist(recipe: Dict) -> List[str]:
    source_config = recipe.get("source", {}).get("config", {})
    schema_section = source_config.get("schema_pattern")
    if isinstance(schema_section, dict):
        candidates = schema_section.get("allow") or []
    else:
        candidates = schema_section or []
    if isinstance(candidates, str):
        candidates = [candidates]
    return [str(value).strip() for value in candidates if str(value).strip()]


def prepare_recipe(recipe: Dict, execution_urn: str) -> Dict:
    prepared = copy.deepcopy(recipe)
    sanitize_host(prepared)
    run_id = execution_urn.split(":")[-1]
    prepared["run_id"] = run_id
    if not prepared.get("pipeline_name"):
        prepared["pipeline_name"] = recipe.get("pipeline_name") or f"ui-{run_id}"
    prepared.setdefault("reporting", [])
    sink_config = prepared.setdefault("sink", {}).setdefault("config", {})
    sink_config.setdefault("server", DEFAULT_SINK_SERVER)
    LOGGER.info(
        "Prepared recipe for %s (pipeline=%s)",
        execution_urn,
        prepared.get("pipeline_name"),
    )
    return prepared


def trigger_tokenization(recipe: Dict) -> None:
    source_config = recipe.get("source", {}).get("config", {})
    host, _, port = (source_config.get("host_port", ":").partition(":"))
    try:
        port_value = int(port) if port else None
    except ValueError:
        LOGGER.warning("Invalid port '%s' in recipe; skipping explicit port override", port)
        port_value = None
    overrides = RuntimeOverrides(
        pipeline_name=recipe.get("pipeline_name"),
        database_host=host or None,
        database_port=port_value,
        database_name=source_config.get("database"),
        database_user=source_config.get("username"),
        database_password=source_config.get("password"),
        schema_allowlist=extract_schema_allowlist(recipe),
    )
    LOGGER.info(
        "Triggering Base64 tokenization for pipeline %s targeting database %s",
        overrides.pipeline_name,
        overrides.database_name,
    )
    action_config = ActionConfig.load(overrides=overrides)
    action = Base64EncodeAction(action_config)
    try:
        action.process_once()
    finally:
        action.conn.close()
    LOGGER.info("Tokenization for pipeline %s finished", overrides.pipeline_name)


def verify_postgres_connection(recipe: Dict) -> None:
    source = recipe.get("source", {})
    if str(source.get("type", "")).lower() != "postgres":
        return
    source_config = source.get("config", {})
    host_port = source_config.get("host_port") or ""
    host, _, port_text = host_port.partition(":")
    if not host:
        host = source_config.get("host") or os.environ.get(
            "UI_RUNNER_DEFAULT_DB_HOST", "postgres"
        )
    if not port_text:
        port_text = str(source_config.get("port") or os.environ.get("UI_RUNNER_DEFAULT_DB_PORT", "5432"))
    try:
        port = int(port_text)
    except ValueError as exc:
        raise RuntimeError(f"Invalid Postgres port '{port_text}' in recipe") from exc
    database = source_config.get("database") or source_config.get("dbname")
    username = source_config.get("username") or source_config.get("user")
    password = source_config.get("password")
    missing = [field for field, value in [("database", database), ("username", username)] if not value]
    if missing:
        raise RuntimeError(
            "Postgres smoke test missing required config values: " + ", ".join(missing)
        )
    LOGGER.info(
        "Running Postgres connectivity check against %s:%s/%s", host, port, database
    )
    try:
        with psycopg.connect(
            host=host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except Exception as exc:  # pylint: disable=broad-except
        raise RuntimeError(
            "Unable to connect to Postgres at "
            f"{host}:{port} for database '{database}' ({exc.__class__.__name__}: {exc})"
        ) from exc
    LOGGER.info(
        "Connectivity check succeeded for Postgres database %s at %s:%s",
        database,
        host,
        port,
    )


def run_ingestion(recipe: Dict, execution_urn: str, executor_id: str) -> int:
    prepared = prepare_recipe(recipe, execution_urn)
    try:
        verify_postgres_connection(prepared)
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.error(
            "Pre-ingestion Postgres connectivity test failed for %s: %s",
            execution_urn,
            exc,
        )
        return 3
    LOGGER.info(
        "Starting ingestion for %s using executor %s (pipeline=%s)",
        execution_urn,
        executor_id,
        prepared.get("pipeline_name"),
    )
    pipeline = Pipeline.create(prepared, report_to="datahub", no_progress=True)
    try:
        pipeline.run()
        pipeline.raise_from_status()
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.exception("Ingestion for %s failed: %s", execution_urn, exc)
        return 1
    finally:
        pipeline.teardown()
    try:
        trigger_tokenization(prepared)
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.exception(
            "Tokenization for %s failed after ingestion: %s", execution_urn, exc
        )
        return 2
    LOGGER.info("Ingestion and tokenization for %s completed successfully", execution_urn)
    return 0


def main() -> int:
    client = GraphQLClient(f"{GMS_URL}/api/graphql")
    handled: set[str] = set()
    LOGGER.info(
        "Starting poller against %s (interval=%ss, executor=%s)",
        GMS_URL,
        POLL_SECONDS,
        DEFAULT_EXECUTOR_ID,
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
                    LOGGER.info(
                        "Detected pending execution %s for source %s",
                        execution_urn,
                        source.get("name", source_urn),
                    )
                    recipe, requested_executor = load_execution_recipe(client, execution_urn)
                    executor_id = requested_executor or DEFAULT_EXECUTOR_ID
                    exit_code = run_ingestion(recipe, execution_urn, executor_id)
                    status = "SUCCEEDED" if exit_code == 0 else f"FAILED({exit_code})"
                    LOGGER.info(
                        "Execution %s finished with status %s",
                        execution_urn,
                        status,
                    )
                    handled.add(execution_urn)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.exception("Error while processing executions: %s", exc)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
