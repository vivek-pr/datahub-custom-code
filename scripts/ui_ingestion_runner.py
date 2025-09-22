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
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

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

DEFAULT_ACTOR = "urn:li:corpuser:ui_ingestion_runner"
GMS_URL_ENV = (os.environ.get("DATAHUB_GMS_URI") or os.environ.get("DATAHUB_GMS_URL"))
GMS_URL = (GMS_URL_ENV or "http://datahub-gms:8080").rstrip("/")
DATAHUB_TOKEN = os.environ.get("DATAHUB_TOKEN")
DATAHUB_ACTOR = os.environ.get("DATAHUB_ACTOR", DEFAULT_ACTOR)
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


SENSITIVE_KEYS = {"password", "secret", "token", "apikey", "api_key", "auth"}


class AuthenticationRequiredError(RuntimeError):
    """Raised when authentication is required but missing."""


class GraphQLRequestError(RuntimeError):
    def __init__(self, message: str, errors: Optional[List[Dict[str, Any]]] = None) -> None:
        super().__init__(message)
        self.errors = errors or []

    def log_details(self) -> None:
        for error in self.errors:
            if not isinstance(error, dict):
                continue
            extensions = error.get("extensions") or {}
            code = extensions.get("code") or extensions.get("type") or "UNKNOWN"
            message = error.get("message") or "GraphQL error"
            LOGGER.error("GraphQL error (%s): %s", code, message)
            stack = extensions.get("stackTrace") or extensions.get("stacktrace") or extensions.get("stack")
            if stack:
                if isinstance(stack, str):
                    stack_lines = stack.splitlines()
                else:
                    stack_lines = [str(line) for line in stack]
                preview = "\n".join(stack_lines[:20])
                LOGGER.error("Server stack trace (first %d lines):\n%s", min(20, len(stack_lines)), preview)


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: Dict[Any, Any] = {}
        for key, val in value.items():
            key_str = str(key).lower()
            if any(secret in key_str for secret in SENSITIVE_KEYS):
                sanitized[key] = "***"
            else:
                sanitized[key] = _sanitize(val)
        return sanitized
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value


def _log_request(endpoint: str, variables: Optional[Dict[str, Any]]) -> None:
    level = logging.DEBUG if LOGGER.isEnabledFor(logging.DEBUG) else logging.INFO
    LOGGER.log(level, "GraphQL POST %s variables=%s", endpoint, json.dumps(_sanitize(variables or {})))


@dataclass
class GraphQLClient:
    endpoint: str
    session: requests.Session
    base_headers: Dict[str, str]
    max_retries: int = 5

    def query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        if operation_name:
            payload["operationName"] = operation_name
        _log_request(self.endpoint, variables)
        response = self._execute(payload)
        try:
            body = response.json()
        except ValueError as exc:  # includes JSONDecodeError
            raise RuntimeError(f"Invalid JSON response from GraphQL endpoint: {exc}") from exc
        errors = body.get("errors")
        if errors:
            err = GraphQLRequestError(
                f"GraphQL errors returned for {operation_name or 'query'}",
                errors=errors,
            )
            err.log_details()
            raise err
        return body.get("data", {})

    def _execute(self, payload: Dict[str, Any]) -> requests.Response:
        headers = dict(self.base_headers)
        headers.setdefault("Content-Type", "application/json")
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(
                    self.endpoint,
                    json=payload,
                    timeout=30,
                    headers=headers,
                )
                response.raise_for_status()
                return response
            except requests.HTTPError as exc:
                status = getattr(exc.response, "status_code", None)
                if status is None or not (500 <= status < 600):
                    raise
                last_exc = exc
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
            self._sleep_with_backoff(attempt)
        raise RuntimeError("Exceeded maximum retries for GraphQL request") from last_exc

    def _sleep_with_backoff(self, attempt: int) -> None:
        if attempt >= self.max_retries:
            return
        delay = min(30.0, (2 ** (attempt - 1))) + random.uniform(0, 0.5)
        LOGGER.warning(
            "GraphQL request error on attempt %s/%s. Retrying in %.2f seconds",
            attempt,
            self.max_retries,
            delay,
        )
        time.sleep(delay)

    def has_type(self, type_name: str) -> bool:
        try:
            data = self.query(
                """
                query __IntrospectType($name: String!) {
                  __type(name: $name) { name }
                }
                """,
                {"name": type_name},
                operation_name="__IntrospectType",
            )
        except Exception:  # pylint: disable=broad-except
            return False
        return bool(data.get("__type"))


def build_base_headers() -> Dict[str, str]:
    headers = {"X-DataHub-Actor": DATAHUB_ACTOR}
    if DATAHUB_TOKEN:
        headers["Authorization"] = f"Bearer {DATAHUB_TOKEN}"
    return headers


def validate_endpoint(
    session: requests.Session,
    base_url: str,
    base_headers: Dict[str, str],
) -> None:
    for path in ("api/health", "api/graphiql"):
        url = f"{base_url}/{path}"
        try:
            response = session.get(url, headers=base_headers, timeout=10)
        except requests.RequestException as exc:  # pylint: disable=broad-except
            raise RuntimeError(f"Failed to reach {url}: {exc}") from exc
        if response.status_code in {401, 403}:
            if not DATAHUB_TOKEN:
                raise AuthenticationRequiredError(
                    "DataHub GMS requires authentication but DATAHUB_TOKEN is not set."
                )
            raise RuntimeError(
                f"Authentication failed for {url} (status {response.status_code})."
            )
        if response.status_code != 200:
            preview = (response.text or "")[:200].strip()
            raise RuntimeError(
                f"Unexpected status {response.status_code} from {url}: {preview}"
            )


def resolve_gms_url(session: requests.Session, base_headers: Dict[str, str]) -> str:
    candidates: List[str]
    if GMS_URL_ENV:
        candidates = [GMS_URL]
    else:
        candidates = [GMS_URL]
        if GMS_URL != "http://host.docker.internal:8080":
            candidates.append("http://host.docker.internal:8080")
    errors: Dict[str, str] = {}
    for candidate in candidates:
        candidate = candidate.rstrip("/")
        try:
            validate_endpoint(session, candidate, base_headers)
            LOGGER.info("Validated DataHub GMS endpoint at %s", candidate)
            return candidate
        except AuthenticationRequiredError:
            raise
        except RuntimeError as exc:
            errors[candidate] = str(exc)
    details = "; ".join(f"{url}: {error}" for url, error in errors.items())
    raise RuntimeError(
        "Unable to connect to DataHub GMS. Checked: " + (details or "no endpoints")
    )


def _flatten_privileges(raw: Any) -> Set[str]:
    privileges: Set[str] = set()
    if raw is None:
        return privileges
    if isinstance(raw, str):
        privileges.add(raw)
        return privileges
    if isinstance(raw, (list, tuple, set)):
        for item in raw:
            privileges.update(_flatten_privileges(item))
        return privileges
    if isinstance(raw, dict):
        for key in ("type", "privilege", "name", "id", "value"):
            value = raw.get(key)
            if isinstance(value, str):
                privileges.add(value)
        if "privileges" in raw:
            privileges.update(_flatten_privileges(raw.get("privileges")))
        return privileges
    return privileges


def _extract_privileges_payload(value: Any) -> Tuple[Set[str], bool]:
    if value is None:
        return set(), False
    is_superuser = False
    privileges = set()
    if isinstance(value, dict):
        if value.get("isSuperUser"):
            is_superuser = True
        if "privileges" in value:
            privileges.update(_flatten_privileges(value.get("privileges")))
        else:
            remaining = {k: v for k, v in value.items() if k != "isSuperUser"}
            privileges.update(_flatten_privileges(remaining))
    else:
        privileges.update(_flatten_privileges(value))
    return privileges, is_superuser


def _extract_from_viewer_privileges(data: Dict[str, Any]) -> Optional[Tuple[Set[str], bool]]:
    if "viewerPrivileges" not in data:
        return None
    value = data.get("viewerPrivileges")
    return _extract_privileges_payload(value)


def _extract_from_viewer_permission(data: Dict[str, Any]) -> Optional[Tuple[Set[str], bool]]:
    if "viewerPermission" not in data:
        return None
    privileges = _flatten_privileges(data.get("viewerPermission"))
    return privileges, False


PrivilegeExtractor = Callable[[Dict[str, Any]], Optional[Tuple[Set[str], bool]]]


PRIVILEGE_QUERIES: Sequence[Tuple[str, str, PrivilegeExtractor]] = (
    (
        "ViewerPrivilegesDetailed",
        """
        query ViewerPrivilegesDetailed {
          viewerPrivileges {
            isSuperUser
            privileges {
              type
              privilege
              name
            }
          }
        }
        """,
        _extract_from_viewer_privileges,
    ),
    (
        "ViewerPrivilegesStrings",
        """
        query ViewerPrivilegesStrings {
          viewerPrivileges {
            isSuperUser
            privileges
          }
        }
        """,
        _extract_from_viewer_privileges,
    ),
    (
        "ViewerPermissionLegacy",
        """
        query ViewerPermissionLegacy {
          viewerPermission {
            type
            privilege
            name
          }
        }
        """,
        _extract_from_viewer_permission,
    ),
)


def fetch_viewer_privileges(client: GraphQLClient) -> Tuple[Set[str], bool]:
    errors: List[str] = []
    for operation_name, query, extractor in PRIVILEGE_QUERIES:
        try:
            data = client.query(query, operation_name=operation_name)
        except GraphQLRequestError as exc:
            LOGGER.warning("Privilege query %s failed: %s", operation_name, exc)
            errors.append(f"{operation_name}: {exc}")
            continue
        result = extractor(data)
        if result is not None:
            privileges, is_superuser = result
            LOGGER.debug(
                "Privileges from %s: %s (superuser=%s)",
                operation_name,
                sorted(privileges),
                is_superuser,
            )
            return privileges, is_superuser
    details = "; ".join(errors)
    raise RuntimeError(
        "Unable to determine viewer privileges from DataHub GMS" + (f": {details}" if details else "")
    )


def ensure_manage_metadata_privilege(client: GraphQLClient) -> None:
    privileges, is_superuser = fetch_viewer_privileges(client)
    if is_superuser or "MANAGE_METADATA_INGESTION" in privileges:
        LOGGER.info("Actor %s authorized for metadata ingestion", DATAHUB_ACTOR)
        return
    remediation = (
        "Actor %s lacks MANAGE_METADATA_INGESTION. Grant this privilege via DataHub roles or "
        "use a token with the required access."
    )
    message = remediation % DATAHUB_ACTOR
    LOGGER.error(message)
    raise PermissionError(message)


LIST_SOURCES_V2 = """
query ListSourcesV2($input: ListIngestionSourcesInput!) {
  listIngestionSources(input: $input) {
    start
    count
    total
    ingestionSources {
      urn
      name
      type
      config { recipe }
      platform { name }
      lastRun { status }
    }
  }
}
"""


LIST_SOURCES_LEGACY = """
query ListSourcesLegacy($start: Int!, $count: Int!, $query: String) {
  listIngestionSources(start: $start, count: $count, query: $query) {
    start
    count
    total
    ingestionSources {
      urn
      name
      type
      config { recipe }
      platform { name }
      lastRun { status }
    }
  }
}
"""


class IngestionSourceLister:
    def __init__(self, client: GraphQLClient) -> None:
        self.client = client
        self._prefers_v2: Optional[bool] = None
        self._logged_fallback = False

    def list_sources(self) -> Sequence[Dict[str, Any]]:
        sources: List[Dict[str, Any]] = []
        start = 0
        while True:
            page = self._fetch_page(start)
            if not page:
                break
            sources.extend(page)
            if len(page) < PAGE_SIZE:
                break
            start += PAGE_SIZE
        return sources

    def _fetch_page(self, start: int) -> Sequence[Dict[str, Any]]:
        prefers_v2 = self._should_use_v2()
        errors: List[Exception] = []
        if prefers_v2:
            try:
                return self._run_v2_query(start)
            except GraphQLRequestError as exc:
                errors.append(exc)
                LOGGER.warning(
                    "listIngestionSources v1.2+ signature failed, attempting legacy fallback: %s",
                    exc,
                )
        try:
            result = self._run_legacy_query(start)
            if prefers_v2 and not self._logged_fallback:
                LOGGER.info("Falling back to legacy listIngestionSources signature")
                self._logged_fallback = True
            return result
        except GraphQLRequestError as exc:
            errors.append(exc)
            messages = " | ".join(str(err) for err in errors)
            raise RuntimeError(
                "listIngestionSources failed using both query signatures: " + messages
            ) from exc

    def _run_v2_query(self, start: int) -> Sequence[Dict[str, Any]]:
        data = self.client.query(
            LIST_SOURCES_V2,
            {"input": {"start": start, "count": PAGE_SIZE, "query": ""}},
            operation_name="ListSourcesV2",
        )
        return self._extract_sources(data)

    def _run_legacy_query(self, start: int) -> Sequence[Dict[str, Any]]:
        data = self.client.query(
            LIST_SOURCES_LEGACY,
            {"start": start, "count": PAGE_SIZE, "query": ""},
            operation_name="ListSourcesLegacy",
        )
        return self._extract_sources(data)

    def _extract_sources(self, data: Dict[str, Any]) -> Sequence[Dict[str, Any]]:
        wrapper = data.get("listIngestionSources") or {}
        return wrapper.get("ingestionSources") or []

    def _should_use_v2(self) -> bool:
        if self._prefers_v2 is None:
            self._prefers_v2 = self.client.has_type("ListIngestionSourcesInput")
            LOGGER.info(
                "Detected listIngestionSources signature: %s",
                "v1.2+ input" if self._prefers_v2 else "legacy arguments",
            )
        return self._prefers_v2


def list_ingestion_sources(client: GraphQLClient) -> Sequence[Dict[str, Any]]:
    return IngestionSourceLister(client).list_sources()


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
    data = client.query(
        query,
        {"urn": source_urn, "start": 0, "count": PAGE_SIZE},
        operation_name="PendingExecutions",
    )
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
    data = client.query(query, {"urn": execution_urn}, operation_name="GetExecution")
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
    session = requests.Session()
    base_headers = build_base_headers()
    session.headers.update(base_headers)
    LOGGER.info("Using DataHub actor %s", base_headers.get("X-DataHub-Actor"))
    if DATAHUB_TOKEN:
        LOGGER.info("Using bearer token authentication for GMS requests")
    else:
        LOGGER.info("No DATAHUB_TOKEN provided; assuming open GMS endpoint")
    try:
        resolved_url = resolve_gms_url(session, base_headers)
    except AuthenticationRequiredError as exc:
        LOGGER.error("%s", exc)
        return 1
    except RuntimeError as exc:
        LOGGER.error("Failed to validate GMS endpoint: %s", exc)
        return 1

    global GMS_URL, DEFAULT_SINK_SERVER  # pylint: disable=global-statement
    GMS_URL = resolved_url
    if not os.environ.get("UI_RUNNER_SINK_SERVER"):
        DEFAULT_SINK_SERVER = GMS_URL

    client = GraphQLClient(f"{GMS_URL}/api/graphql", session=session, base_headers=base_headers)
    try:
        ensure_manage_metadata_privilege(client)
    except PermissionError:
        return 1
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.error("Failed to verify viewer privileges: %s", exc)
        return 1

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
