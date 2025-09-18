#!/usr/bin/env python3
"""POC verifier for the Minikube DataHub proof-of-concept stack."""
from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as _dt
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

try:  # pragma: no cover - dependency availability checked at runtime
    import requests
except ImportError as _import_err:  # pragma: no cover - handled gracefully
    requests = None  # type: ignore
    _REQUESTS_ERROR = _import_err
else:  # pragma: no cover - executed during normal runtime
    _REQUESTS_ERROR = None


TOKEN_PATTERN = re.compile(r"^tok_[0-9a-f]{8,}_poc$")
DEFAULT_REQUEST_ID = "poc-smoke"
DEFAULT_TENANT = "t001"
DEFAULT_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,sandbox.t001.customers,PROD)"
)
REQUIRED_WORKLOAD_PATTERNS = (
    "datahub-gms",
    "datahub-frontend",
    "datahub-mae-consumer",
    "postgres",
    "classifier",
    "actions",
)

PG_SECRET_NAME = "pg-secrets"

TOKENIZATION_RUNS_QUERY = """
    query TokenizationRuns($urn: String!, $request: String!) {
      tokenizationRuns(urn: $urn, requestId: $request) {
        requestId
        status
        rowsAffected
        runUrn
        context {
          requestId
          tenantId
          externalUrl
          columns {
            fieldPath
            rowsAffected
          }
        }
      }
    }
"""


def parse_postgres_dataset_urn(urn: str) -> Tuple[str, str, str]:
    """Return (database, schema, table) for a Postgres dataset URN."""

    match = re.match(r"urn:li:dataset:\(([^,]+),([^,]+),([^)]+)\)", urn)
    if not match:
        raise VerificationError(
            "Unsupported dataset URN format",
            context={"urn": urn},
        )
    platform_urn, dataset_name, _ = match.groups()
    if not platform_urn.endswith(":postgres"):
        raise VerificationError(
            "Dataset URN does not reference Postgres",
            context={"urn": urn},
        )
    parts = dataset_name.split(".")
    if len(parts) < 2:
        raise VerificationError(
            "Dataset name missing schema/table",
            context={"dataset": dataset_name},
        )
    table = parts[-1]
    schema = parts[-2]
    if len(parts) > 2:
        database = ".".join(parts[:-2])
    else:
        database = parts[0]
    return database, schema, table


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


class VerificationError(RuntimeError):
    """Error raised when a verification step fails."""

    def __init__(self, message: str, *, context: Optional[Mapping[str, Any]] = None):
        super().__init__(message)
        self.context = dict(context or {})


@dataclass
class StepResult:
    """Structured record of a single verification step."""

    name: str
    status: str
    duration: float
    detail: str
    data: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "duration": round(self.duration, 3),
            "detail": self.detail,
            "data": self.data,
        }


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def is_condition_true(conditions: Sequence[Mapping[str, Any]], condition_type: str) -> bool:
    for condition in conditions:
        if condition.get("type") == condition_type:
            return condition.get("status") == "True"
    return False


def is_pod_ready(pod: Mapping[str, Any]) -> bool:
    conditions = pod.get("status", {}).get("conditions") or []
    ready = is_condition_true(conditions, "Ready")
    if not ready:
        return False
    container_statuses = pod.get("status", {}).get("containerStatuses") or []
    for status in container_statuses:
        if not status.get("ready"):
            return False
    return True


def extract_schema_field_tags(dataset_payload: Mapping[str, Any]) -> Dict[str, List[str]]:
    """Return mapping of schema field URN -> tags from GraphQL payload."""

    result: Dict[str, List[str]] = {}
    editable = dataset_payload.get("editableSchemaMetadata") or {}
    fields = editable.get("editableSchemaFieldInfo") or []
    for field in fields:
        field_urn = field.get("fieldPath") or field.get("fieldPathType") or field.get("schemaFieldUrn")
        tags = []
        tag_props = field.get("globalTags") or {}
        for tag in tag_props.get("tags", []):
            urn = tag.get("tag") or tag.get("tagUrn")
            if urn:
                tags.append(urn)
        if field_urn:
            result[field_urn] = tags
    return result


def ensure_unique_tags(tag_sets: Mapping[str, Iterable[str]]) -> Dict[str, List[str]]:
    """Remove duplicate tags preserving order per field."""

    deduped: Dict[str, List[str]] = {}
    for field, tags in tag_sets.items():
        seen: set[str] = set()
        ordered: List[str] = []
        for tag in tags:
            if tag not in seen:
                ordered.append(tag)
                seen.add(tag)
        deduped[field] = ordered
    return deduped


def is_tokenized_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return False
    if isinstance(value, bytes):
        try:
            value = value.decode()
        except Exception:
            return False
    text = str(value).strip()
    return bool(TOKEN_PATTERN.match(text))


def summarize_tokenization(before: Sequence[Any], after: Sequence[Any]) -> MutableMapping[str, int]:
    if len(before) != len(after):
        raise ValueError("Comparisons must have identical lengths")
    summary = {"updated": 0, "unchanged": 0, "tokenized": 0}
    for pre, post in zip(before, after):
        if pre == post:
            summary["unchanged"] += 1
        else:
            summary["updated"] += 1
        if is_tokenized_value(post):
            summary["tokenized"] += 1
    return summary


def evaluate_run_transitions(states: Sequence[str], *, expect_success: bool = True) -> Dict[str, Any]:
    if not states:
        raise VerificationError("No run states supplied", context={"states": []})
    seen = []
    terminal = {"COMPLETED", "FAILED", "SUCCESS", "SUCCESSFUL"}
    for state in states:
        normalized = state.upper()
        if not seen:
            if normalized not in {"RUNNING", "PENDING", "ENQUEUED"}:
                raise VerificationError(
                    "Run did not begin in a pending/running state",
                    context={"states": states},
                )
        else:
            last = seen[-1]
            if last == "RUNNING" and normalized not in terminal | {"RUNNING"}:
                raise VerificationError(
                    "Invalid transition from RUNNING",
                    context={"states": states},
                )
        seen.append(normalized)
    final = seen[-1]
    if expect_success and final not in {"SUCCESS", "COMPLETED", "SUCCESSFUL"}:
        raise VerificationError(
            "Run did not reach a successful terminal state",
            context={"states": states},
        )
    if not expect_success and final not in {"FAILED", "FAILURE"}:
        raise VerificationError(
            "Negative run did not fail as expected",
            context={"states": states},
        )
    return {"states": seen, "final": final}


class POCVerifier:
    def __init__(
        self,
        *,
        namespace: str,
        tenant: str,
        dataset_urn: str,
        timeout: int,
        artifacts_dir: Path,
        expect_idempotent: bool,
        request_id: str,
    ) -> None:
        self.namespace = namespace
        self.tenant = tenant
        self.dataset_urn = dataset_urn
        self.timeout = timeout
        self.artifacts_dir = artifacts_dir
        self.expect_idempotent = expect_idempotent
        self.request_id = request_id
        self.verify_dir = artifacts_dir / "verify"
        ensure_dir(self.verify_dir)
        self.database, self.schema, self.table = parse_postgres_dataset_urn(dataset_urn)
        self._pg_credentials: Dict[str, str] = {}
        self._pg_admin_user = os.environ.get("POC_PG_SUPERUSER", "postgres")
        self._postgres_pod: Optional[str] = None
        self._snapshot_before: Optional[List[Dict[str, Any]]] = None
        self._snapshot_after: Optional[List[Dict[str, Any]]] = None
        self._pii_columns: Set[str] = set()
        self._last_run_context: Dict[str, Any] = {}
        self._tokenization_query_extended_supported = True
        self._known_tenants: List[str] = []
        self._tokenization_summary: Dict[str, Dict[str, int]] = {}

    # -------- Utility helpers ---------
    def _run(self, *cmd: str, capture_output: bool = True, check: bool = True) -> subprocess.CompletedProcess:
        try:
            proc = subprocess.run(
                cmd,
                check=check,
                capture_output=capture_output,
                text=True,
            )
            return proc
        except subprocess.CalledProcessError as exc:  # pragma: no cover - exercised indirectly
            context = {
                "cmd": list(cmd),
                "returncode": exc.returncode,
                "stdout": exc.stdout,
                "stderr": exc.stderr,
            }
            raise VerificationError(f"Command failed: {' '.join(cmd)}", context=context) from exc

    def _kubectl_json(self, *extra: str) -> Mapping[str, Any]:
        proc = self._run("kubectl", "-n", self.namespace, *extra)
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise VerificationError(
                "kubectl did not return valid JSON",
                context={"output": proc.stdout},
            ) from exc

    def _kubectl(self, *extra: str) -> subprocess.CompletedProcess:
        return self._run("kubectl", "-n", self.namespace, *extra)

    def _resolve_service_url(self, *svc_names: str) -> str:
        env = os.environ.get("DATAHUB_GMS")
        if env:
            return env.rstrip("/")
        profile = os.environ.get("MINIKUBE_PROFILE", "minikube")
        for svc in svc_names:
            try:
                proc = self._run(
                    "minikube",
                    "-p",
                    profile,
                    "service",
                    "-n",
                    self.namespace,
                    svc,
                    "--url",
                )
            except VerificationError:
                continue
            url = proc.stdout.strip().splitlines()
            if url:
                return url[0].rstrip("/")
        raise VerificationError(
            "Unable to resolve GMS URL",
            context={"namespace": self.namespace, "services": list(svc_names)},
        )

    def _http_get(self, url: str, *, timeout: int = 10) -> Any:
        if requests is None:  # pragma: no cover - handled when dependency missing
            raise VerificationError(
                "requests dependency not installed",
                context={"error": str(_REQUESTS_ERROR) if _REQUESTS_ERROR else None},
            )
        try:
            resp = requests.get(url, timeout=timeout)
            return resp
        except requests.RequestException as exc:
            raise VerificationError(f"HTTP GET failed for {url}", context={"error": str(exc)}) from exc
        except Exception as exc:  # pragma: no cover - defensive
            raise VerificationError(f"HTTP GET failed for {url}", context={"error": str(exc)}) from exc

    def _http_post(self, url: str, *, json_payload: Mapping[str, Any], timeout: int = 15) -> Mapping[str, Any]:
        if requests is None:  # pragma: no cover - handled when dependency missing
            raise VerificationError(
                "requests dependency not installed",
                context={"error": str(_REQUESTS_ERROR) if _REQUESTS_ERROR else None},
            )
        try:
            resp = requests.post(url, json=json_payload, timeout=timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise VerificationError(
                f"HTTP POST failed for {url}",
                context={"payload": json_payload, "error": str(exc)},
            ) from exc
        except Exception as exc:  # pragma: no cover - defensive
            raise VerificationError(
                f"HTTP POST failed for {url}",
                context={"payload": json_payload, "error": str(exc)},
            ) from exc
        try:
            return resp.json()
        except ValueError as exc:
            raise VerificationError(
                "Response was not JSON",
                context={"text": resp.text},
            ) from exc

    # -------- Postgres helpers ---------
    def _load_pg_credentials(self) -> Dict[str, str]:
        if self._pg_credentials:
            return self._pg_credentials
        secret = self._kubectl_json("get", "secret", PG_SECRET_NAME, "-o", "json")
        data = secret.get("data") or {}
        if not data:
            raise VerificationError(
                "Postgres secret missing data",
                context={"secret": PG_SECRET_NAME},
            )
        creds: Dict[str, str] = {}
        for key, value in data.items():
            try:
                decoded = base64.b64decode(value).decode()
            except Exception:  # pragma: no cover - defensive
                continue
            upper = key.upper()
            if upper == "POSTGRES_PASSWORD":
                creds[self._pg_admin_user] = decoded
            elif upper.endswith("_PASSWORD"):
                tenant = upper[:-10].lower()
                creds[tenant] = decoded
        if self._pg_admin_user not in creds:
            raise VerificationError(
                "Admin credential not found in secret",
                context={"secret": PG_SECRET_NAME, "user": self._pg_admin_user},
            )
        self._pg_credentials = creds
        return creds

    def _get_postgres_pod(self) -> str:
        if self._postgres_pod:
            return self._postgres_pod
        payload = self._kubectl_json("get", "pods", "-l", "app=postgres", "-o", "json")
        pods = payload.get("items", [])
        if not pods:
            raise VerificationError("Postgres pod not found")
        for pod in pods:
            if is_pod_ready(pod):
                name = pod.get("metadata", {}).get("name")
                if name:
                    self._postgres_pod = name
                    return name
        fallback = pods[0].get("metadata", {}).get("name")
        if not fallback:
            raise VerificationError("Unable to resolve Postgres pod name")
        self._postgres_pod = fallback
        return fallback

    def _get_actions_pod(self) -> Optional[str]:
        payload = self._kubectl_json("get", "pods", "-l", "app=actions-tokenize", "-o", "json")
        pods = payload.get("items", [])
        for pod in pods:
            if is_pod_ready(pod):
                name = pod.get("metadata", {}).get("name")
                if name:
                    return name
        if pods:
            return pods[0].get("metadata", {}).get("name")
        return None

    def _psql(
        self,
        sql: str,
        *,
        username: str,
        expect_success: bool = True,
    ) -> subprocess.CompletedProcess:
        creds = self._load_pg_credentials()
        username_lc = username.lower()
        password = creds.get(username_lc) or creds.get(username)
        if password is None:
            raise VerificationError(
                "No credential available for Postgres user",
                context={"user": username},
            )
        pod = self._get_postgres_pod()
        sql_quoted = shlex.quote(sql)
        password_quoted = shlex.quote(password)
        user_quoted = shlex.quote(username)
        db_quoted = shlex.quote(self.database)
        command = (
            f"PGPASSWORD={password_quoted} psql -v ON_ERROR_STOP=1 -X -A -F',' -P footer=off "
            f"-U {user_quoted} -d {db_quoted} -c {sql_quoted}"
        )
        proc = self._run(
            "kubectl",
            "-n",
            self.namespace,
            "exec",
            pod,
            "--",
            "sh",
            "-lc",
            command,
            capture_output=True,
            check=False,
        )
        if expect_success and proc.returncode != 0:
            raise VerificationError(
                "psql command failed",
                context={
                    "user": username,
                    "code": proc.returncode,
                    "stderr": proc.stderr.strip(),
                },
            )
        if not expect_success and proc.returncode == 0:
            raise VerificationError(
                "psql command succeeded unexpectedly",
                context={"user": username, "sql": sql},
            )
        return proc

    def _fetch_customer_rows(self, *, username: str) -> List[Mapping[str, Any]]:
        columns = ["id"] + sorted(self._pii_columns)
        select_clause = ", ".join(columns)
        sql = (
            "SELECT row_to_json(r) FROM ("
            f"SELECT {select_clause} FROM {self.schema}.{self.table} ORDER BY id"
            ") r;"
        )
        proc = self._psql(sql, username=username)
        rows: List[Mapping[str, Any]] = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows

    def _fetch_tokenization_runs(self, gms: str, request_id: str) -> List[Mapping[str, Any]]:
        payload = {
            "query": TOKENIZATION_RUNS_QUERY,
            "variables": {"urn": self.dataset_urn, "request": request_id},
        }
        data = self._http_post(f"{gms}/api/graphql", json_payload=payload)
        errors = data.get("errors")
        if errors:
            raise VerificationError(
                "GraphQL error retrieving tokenization runs",
                context={"errors": errors},
            )
        runs = ((data.get("data") or {}).get("tokenizationRuns") or [])
        if not isinstance(runs, list):
            raise VerificationError(
                "Unexpected tokenizationRuns payload",
                context={"payload": data},
            )
        return runs

    def _trigger_tokenization(
        self,
        *,
        request_id: str,
        expect_success: bool,
        allow_zero_rows: bool = False,
    ) -> Dict[str, Any]:
        gms = self._resolve_service_url(
            f"{os.environ.get('RELEASE_DATAHUB', 'datahub')}-datahub-gms",
            "datahub-datahub-gms",
            "datahub-gms",
        )
        mutation = {
            "query": """
                mutation Trigger($urn: String!, $request: String!) {
                  addTag(input: {tagUrn: \"urn:li:tag:tokenize-now\", resourceUrn: $urn})
                  triggerTokenization(input: {resourceUrn: $urn, requestId: $request}) {
                    requestId
                    status
                  }
                }
            """,
            "variables": {"urn": self.dataset_urn, "request": request_id},
        }
        response = self._http_post(f"{gms}/api/graphql", json_payload=mutation)
        trigger = ((response.get("data") or {}).get("triggerTokenization") or {})
        req_id = trigger.get("requestId") or request_id
        deadline = time.time() + self.timeout
        states: List[str] = []
        rows_affected: Optional[int] = None
        run_context: Mapping[str, Any] = {}
        run_urn: Optional[str] = None
        while time.time() < deadline:
            runs = self._fetch_tokenization_runs(gms, req_id)
            if runs:
                run = runs[0]
                state = run.get("status") or ""
                if state:
                    states.append(state)
                rows_affected = run.get("rowsAffected", rows_affected)
                run_context = run.get("context") or run_context
                run_urn = run.get("runUrn") or run_urn
                if state.upper() in {"COMPLETED", "SUCCESS", "FAILED", "FAILURE"}:
                    break
            time.sleep(5)
        if not states:
            raise VerificationError(
                "Tokenization run did not report any status",
                context={"requestId": req_id},
            )
        evaluation = evaluate_run_transitions(states, expect_success=expect_success)
        rows_final = 0 if rows_affected is None else rows_affected
        if expect_success:
            if not allow_zero_rows and rows_final <= 0:
                raise VerificationError(
                    "Tokenization run did not update any rows",
                    context={"rowsAffected": rows_final},
                )
            tenant_id = run_context.get("tenantId") if isinstance(run_context, Mapping) else None
            if tenant_id and tenant_id != self.tenant:
                raise VerificationError(
                    "Run context tenant mismatch",
                    context={"tenantId": tenant_id, "expected": self.tenant},
                )
            if isinstance(run_context, Mapping):
                context_request = run_context.get("requestId")
            else:
                context_request = None
            if context_request and context_request != req_id:
                raise VerificationError(
                    "Run context requestId mismatch",
                    context={"contextRequest": context_request, "expected": req_id},
                )
            if context_request is None:
                run_context = dict(run_context) if isinstance(run_context, Mapping) else {}
                run_context["requestId"] = req_id
        else:
            if evaluation["final"] not in {"FAILED", "FAILURE"}:
                raise VerificationError(
                    "Negative run did not fail as expected",
                    context={"states": evaluation["states"]},
                )
            if rows_final != 0:
                raise VerificationError(
                    "Negative run mutated rows",
                    context={"rowsAffected": rows_final},
                )
        return {
            "states": evaluation["states"],
            "final": evaluation["final"],
            "rowsAffected": rows_final,
            "requestId": req_id,
            "runContext": run_context,
            "runUrn": run_urn,
        }

    def _compare_snapshots(
        self,
        before: Sequence[Mapping[str, Any]],
        after: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Dict[str, int]]:
        summary: Dict[str, Dict[str, int]] = {}
        for column in sorted(self._pii_columns):
            before_values = [row.get(column) for row in before]
            after_values = [row.get(column) for row in after]
            summary[column] = summarize_tokenization(before_values, after_values)
        return summary

    def _toggle_tenant_update(self, *, enable: bool) -> None:
        action = "GRANT" if enable else "REVOKE"
        sql = (
            f"{action} UPDATE ON ALL TABLES IN SCHEMA {self.schema} "
            f"{'TO' if enable else 'FROM'} {self.tenant};"
        )
        self._psql(sql, username=self._pg_admin_user)

    def _restore_dataset(self, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        statements = []
        for row in rows:
            row_id = row.get("id")
            if row_id is None:
                continue
            assignments = []
            for column in sorted(self._pii_columns):
                assignments.append(f"{column} = {sql_literal(row.get(column))}")
            assignments_sql = ", ".join(assignments)
            statements.append(
                f"UPDATE {self.schema}.{self.table} SET {assignments_sql} WHERE id = {row_id};"
            )
        if not statements:
            return
        sql = "BEGIN; " + " ".join(statements) + " COMMIT;"
        self._psql(sql, username=self._pg_admin_user)

    # -------- Verification steps ---------
    def verify_cluster(self) -> Dict[str, Any]:
        payload = self._kubectl_json("get", "pods", "-o", "json")
        pods = payload.get("items", [])
        unready = [pod.get("metadata", {}).get("name") for pod in pods if not is_pod_ready(pod)]
        missing = []
        names = [pod.get("metadata", {}).get("name", "") for pod in pods]
        for pattern in REQUIRED_WORKLOAD_PATTERNS:
            if not any(pattern in name for name in names):
                missing.append(pattern)
        if unready or missing:
            raise VerificationError(
                "Cluster workloads are not ready",
                context={"unready": unready, "missing": missing},
            )
        nodes = self._run("kubectl", "get", "nodes", "-o", "json")
        try:
            nodes_json = json.loads(nodes.stdout)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise VerificationError("kubectl get nodes returned invalid JSON") from exc
        not_ready_nodes = []
        for node in nodes_json.get("items", []):
            conditions = node.get("status", {}).get("conditions") or []
            if not is_condition_true(conditions, "Ready"):
                not_ready_nodes.append(node.get("metadata", {}).get("name"))
        if not_ready_nodes:
            raise VerificationError(
                "Not all nodes are Ready",
                context={"nodes": not_ready_nodes},
            )
        return {"pods": names, "nodeCount": len(nodes_json.get("items", []))}

    def verify_datahub(self) -> Dict[str, Any]:
        gms = self._resolve_service_url(
            f"{os.environ.get('RELEASE_DATAHUB', 'datahub')}-datahub-gms",
            "datahub-datahub-gms",
            "datahub-gms",
        )
        health_url = f"{gms}/api/health"
        graphiql_url = f"{gms}/api/graphiql"
        deadline = time.time() + min(self.timeout, 600)
        last_status = None
        while time.time() < deadline:
            resp = self._http_get(graphiql_url, timeout=5)
            last_status = resp.status_code
            if resp.status_code == 200:
                break
            time.sleep(5)
        else:
            raise VerificationError(
                "GraphiQL endpoint not ready",
                context={"status": last_status, "url": graphiql_url},
            )
        graphql_query = {"query": "query Health { health { status message } }"}
        data = self._http_post(f"{gms}/api/graphql", json_payload=graphql_query)
        health = ((data.get("data") or {}).get("health") or {})
        status = (health.get("status") or "").upper()
        if status != "HEALTHY":
            raise VerificationError(
                "GraphQL health check reported unhealthy",
                context={"health": health},
            )
        # Tag roundtrip using GraphQL mutation
        tag_name = f"poc-smoke-{int(time.time())}"
        tag_urn = f"urn:li:tag:{tag_name}"
        create_mutation = {
            "query": """
                mutation CreateTag($urn: String!, $name: String!) {
                  createTag(input: {id: $name, name: $name, description: \"POC verifier\"})
                  addTag(input: {tagUrn: $urn, resourceUrn: $urn})
                }
            """,
            "variables": {"urn": tag_urn, "name": tag_name},
        }
        try:
            self._http_post(f"{gms}/api/graphql", json_payload=create_mutation)
        except VerificationError as exc:
            # Creation might fail if tag already exists; treat as warning but still verify fetch
            sys.stderr.write(f"[verify] warning: could not create tag ({exc})\n")
        fetch_mutation = {
            "query": """
                query GetTag($urn: String!) {
                  tag(urn: $urn) { urn name }
                }
            """,
            "variables": {"urn": tag_urn},
        }
        fetched = self._http_post(f"{gms}/api/graphql", json_payload=fetch_mutation)
        tag_info = ((fetched.get("data") or {}).get("tag") or {})
        if tag_info.get("urn") != tag_urn:
            raise VerificationError(
                "Unable to read back created tag",
                context={"tag": tag_info},
            )
        return {"gms": gms, "tag": tag_info, "health": health}

    def verify_postgres(self) -> Dict[str, Any]:
        creds = self._load_pg_credentials()
        tenants = sorted(
            tenant
            for tenant in creds
            if tenant not in {self._pg_admin_user, self._pg_admin_user.lower()}
        )
        if self.tenant not in tenants:
            raise VerificationError(
                "Target tenant credential missing",
                context={"tenant": self.tenant, "tenants": tenants},
            )
        connectivity: Dict[str, str] = {}
        for tenant in tenants:
            self._psql("SELECT 1;", username=tenant)
            connectivity[tenant] = "ok"
        isolation_checks = []
        for tenant in tenants:
            for other in tenants:
                if tenant == other:
                    continue
                sql = f"SELECT 1 FROM {other}.{self.table} LIMIT 1;"
                proc = self._psql(sql, username=tenant, expect_success=False)
                stderr = (proc.stderr or "").lower()
                denied = "permission denied" in stderr
                missing = "does not exist" in stderr or "not exist" in stderr
                if not (denied or missing):
                    raise VerificationError(
                        "Cross-tenant access was not blocked",
                        context={
                            "from": tenant,
                            "to": other,
                            "stderr": proc.stderr.strip(),
                        },
                    )
                isolation_checks.append({"from": tenant, "to": other, "status": "blocked"})
        self._known_tenants = tenants
        return {
            "pod": self._get_postgres_pod(),
            "tenants": tenants,
            "isolationChecks": isolation_checks,
        }

    def verify_dataset_metadata(self) -> Dict[str, Any]:
        gms = self._resolve_service_url(
            f"{os.environ.get('RELEASE_DATAHUB', 'datahub')}-datahub-gms",
            "datahub-datahub-gms",
            "datahub-gms",
        )
        query = {
            "query": """
                query Dataset($urn: String!) {
                  dataset(urn: $urn) {
                    urn
                    name
                    editableSchemaMetadata {
                      editableSchemaFieldInfo {
                        fieldPath
                        globalTags { tags { tag } }
                      }
                    }
                    schemaMetadata(version: 0) {
                      schemaName
                    }
                  }
                }
            """,
            "variables": {"urn": self.dataset_urn},
        }
        data = self._http_post(f"{gms}/api/graphql", json_payload=query)
        dataset = (data.get("data") or {}).get("dataset")
        if not dataset:
            raise VerificationError(
                "Dataset not returned from GraphQL",
                context={"datasetUrn": self.dataset_urn, "response": data},
            )
        tags = ensure_unique_tags(extract_schema_field_tags(dataset))
        pii_tagged = {
            field: [tag for tag in values if tag.split(":")[-1].startswith("pii-")]
            for field, values in tags.items()
        }
        has_pii = any(pii_tagged.values())
        if not has_pii:
            raise VerificationError(
                "Classifier tags missing",
                context={"tags": tags},
            )
        pii_columns = {field.split(".")[-1] for field, values in pii_tagged.items() if values}
        if not pii_columns:
            raise VerificationError(
                "Unable to resolve PII columns from tags",
                context={"pii": pii_tagged},
            )
        self._pii_columns = pii_columns
        if self._snapshot_before is None:
            self._snapshot_before = self._fetch_customer_rows(username=self.tenant)
        dataset_name = dataset.get("name")
        return {
            "dataset": dataset.get("urn"),
            "datasetName": dataset_name,
            "piiFields": {field: values for field, values in pii_tagged.items() if values},
            "piiColumns": sorted(self._pii_columns),
        }

    def verify_tokenization(self) -> Dict[str, Any]:
        if not self._pii_columns:
            raise VerificationError("PII columns not discovered; run metadata step first")
        if self._snapshot_before is None:
            self._snapshot_before = self._fetch_customer_rows(username=self.tenant)
        before = self._snapshot_before or []
        result = self._trigger_tokenization(
            request_id=self.request_id,
            expect_success=True,
            allow_zero_rows=False,
        )
        after = self._fetch_customer_rows(username=self.tenant)
        if len(before) != len(after):
            raise VerificationError(
                "Row count changed after tokenization",
                context={"before": len(before), "after": len(after)},
            )
        summary = self._compare_snapshots(before, after)
        updated_rows = 0
        for before_row, after_row in zip(before, after):
            changed = any(
                before_row.get(column) != after_row.get(column)
                for column in self._pii_columns
            )
            if changed:
                updated_rows += 1
                for column in self._pii_columns:
                    if before_row.get(column) == after_row.get(column):
                        continue
                    if not is_tokenized_value(after_row.get(column)):
                        raise VerificationError(
                            "Column value not tokenized with expected pattern",
                            context={"column": column},
                        )
        if result["rowsAffected"] < updated_rows:
            raise VerificationError(
                "Reported rowsAffected lower than observed updates",
                context={
                    "reported": result["rowsAffected"],
                    "observed": updated_rows,
                },
            )
        context = result.get("runContext") or {}
        columns_ctx = context.get("columns") if isinstance(context, Mapping) else None
        if isinstance(columns_ctx, str):
            with contextlib.suppress(json.JSONDecodeError):
                columns_ctx = json.loads(columns_ctx)
        column_summary = []
        if isinstance(columns_ctx, list):
            for entry in columns_ctx:
                if isinstance(entry, Mapping):
                    column_summary.append(
                        {
                            "fieldPath": entry.get("fieldPath"),
                            "rowsAffected": entry.get("rowsAffected"),
                        }
                    )
        else:
            raise VerificationError(
                "Run context missing column summary",
                context={"context": context},
            )
        for entry in column_summary:
            field_path = entry.get("fieldPath")
            if not field_path:
                continue
            column = field_path.split(".")[-1]
            expected = summary.get(column, {}).get("updated")
            if expected is None:
                continue
            rows_value = entry.get("rowsAffected")
            if isinstance(rows_value, str):
                with contextlib.suppress(ValueError):
                    rows_value = int(rows_value)
            if rows_value != expected:
                raise VerificationError(
                    "Run context rowsAffected mismatch",
                    context={
                        "column": column,
                        "expected": expected,
                        "context": rows_value,
                    },
                )
        tenant_in_context = context.get("tenantId") if isinstance(context, Mapping) else None
        if not tenant_in_context:
            raise VerificationError("Run context missing tenantId", context={"context": context})
        self._snapshot_after = after
        self._tokenization_summary = summary
        self._last_run_context = {
            "requestId": result.get("requestId"),
            "runUrn": result.get("runUrn"),
            "tenantId": tenant_in_context,
            "externalUrl": context.get("externalUrl"),
            "columns": column_summary,
        }
        return {
            "states": result["states"],
            "final": result["final"],
            "rowsAffected": result["rowsAffected"],
            "updatedRowsObserved": updated_rows,
            "runUrn": result.get("runUrn"),
            "context": self._last_run_context,
            "tokenizedColumns": summary,
        }

    def verify_idempotency(self) -> Dict[str, Any]:
        if not self.expect_idempotent:
            return {"skipped": True}
        if self._snapshot_after is None:
            raise VerificationError("Tokenization snapshot missing for idempotency validation")
        result = self._trigger_tokenization(
            request_id=self.request_id,
            expect_success=True,
            allow_zero_rows=True,
        )
        after = self._fetch_customer_rows(username=self.tenant)
        summary = self._compare_snapshots(self._snapshot_after, after)
        for column, metrics in summary.items():
            if metrics["updated"] != 0:
                raise VerificationError(
                    "Idempotent re-run mutated column",
                    context={"column": column, "summary": metrics},
                )
        self._snapshot_after = after
        context = result.get("runContext") or {}
        columns_ctx = context.get("columns") if isinstance(context, Mapping) else None
        if isinstance(columns_ctx, str):
            with contextlib.suppress(json.JSONDecodeError):
                columns_ctx = json.loads(columns_ctx)
        if result["rowsAffected"] != 0:
            raise VerificationError(
                "Idempotent re-run reported non-zero rows",
                context={"rowsAffected": result["rowsAffected"]},
            )
        zero_columns = []
        if isinstance(columns_ctx, list):
            for entry in columns_ctx:
                if isinstance(entry, Mapping):
                    rows_value = entry.get("rowsAffected")
                    normalized = rows_value
                    if isinstance(rows_value, str):
                        with contextlib.suppress(ValueError):
                            normalized = int(rows_value)
                    if normalized not in (0, None):
                        raise VerificationError(
                            "Idempotent run reported column updates",
                            context={
                                "fieldPath": entry.get("fieldPath"),
                                "rowsAffected": normalized,
                            },
                        )
                    zero_columns.append(
                        {
                            "fieldPath": entry.get("fieldPath"),
                            "rowsAffected": normalized,
                        }
                    )
        return {
            "states": result["states"],
            "final": result["final"],
            "rowsAffected": result["rowsAffected"],
            "columns": zero_columns,
        }

    def verify_negative_path(self) -> Dict[str, Any]:
        if self._snapshot_before is None or not self._snapshot_before:
            raise VerificationError("Baseline snapshot missing before negative test")
        self._restore_dataset(self._snapshot_before)
        restored = self._fetch_customer_rows(username=self.tenant)
        if restored != self._snapshot_before:
            raise VerificationError(
                "Failed to restore dataset to baseline before negative test",
                context={"expected": len(self._snapshot_before), "actual": len(restored)},
            )
        negative_request = f"{self.request_id}-negative"
        self._toggle_tenant_update(enable=False)
        try:
            negative_result = self._trigger_tokenization(
                request_id=negative_request,
                expect_success=False,
                allow_zero_rows=True,
            )
        finally:
            self._toggle_tenant_update(enable=True)
        after_failure = self._fetch_customer_rows(username=self.tenant)
        if after_failure != self._snapshot_before:
            raise VerificationError(
                "Negative run mutated data despite permission drop",
                context={"rows": len(after_failure)},
            )
        recovery_request = f"{self.request_id}-recovery"
        recovery_result = self._trigger_tokenization(
            request_id=recovery_request,
            expect_success=True,
            allow_zero_rows=False,
        )
        recovery_snapshot = self._fetch_customer_rows(username=self.tenant)
        summary = self._compare_snapshots(self._snapshot_before, recovery_snapshot)
        for column, metrics in summary.items():
            if metrics["tokenized"] == 0:
                raise VerificationError(
                    "Recovery run did not tokenize column",
                    context={"column": column, "summary": metrics},
                )
        self._snapshot_after = recovery_snapshot
        self._tokenization_summary = summary
        return {
            "negative": {
                "states": negative_result["states"],
                "final": negative_result["final"],
                "rowsAffected": negative_result["rowsAffected"],
                "runUrn": negative_result.get("runUrn"),
            },
            "recovery": {
                "states": recovery_result["states"],
                "final": recovery_result["final"],
                "rowsAffected": recovery_result["rowsAffected"],
                "runUrn": recovery_result.get("runUrn"),
            },
        }

    def verify_observability(self) -> Dict[str, Any]:
        logs_dir = self.artifacts_dir / "logs"
        ensure_dir(logs_dir)
        proc = self._kubectl("get", "pods", "-o", "jsonpath={range .items[*]}{.metadata.name}\n{end}")
        pods = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
        collected: List[str] = []
        for pod in pods:
            if not any(token in pod for token in ("datahub", "postgres", "classifier", "actions")):
                continue
            log_file = logs_dir / f"{pod}.log"
            try:
                logs = self._kubectl("logs", pod)
            except VerificationError:
                continue
            log_file.write_text(logs.stdout)
            collected.append(str(log_file))
        actions_logs = [Path(path) for path in collected if "actions-tokenize" in path]
        has_run_completed = False
        has_request_id = False
        for log_path in actions_logs:
            for line in log_path.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                event = payload.get("event")
                if event == "run.completed" and "tenant_id" in payload and "rows_affected" in payload:
                    has_run_completed = True
                    if "request_id" in payload:
                        has_request_id = True
                if "request_id" in payload:
                    has_request_id = True
                if has_run_completed and has_request_id:
                    break
            if has_run_completed and has_request_id:
                break
        request_id_context = self._last_run_context.get("requestId")
        if not has_run_completed:
            raise VerificationError("Actions logs missing run.completed entries with tenant context")
        if not has_request_id and not request_id_context:
            raise VerificationError("Request identifier missing from logs and run context")
        metrics_status: Dict[str, Any] = {"enabled": False}
        metrics_port: Optional[str] = None
        try:
            deploy = self._kubectl_json("get", "deploy", "actions-tokenize", "-o", "json")
        except VerificationError:
            deploy = {}
        containers = ((deploy.get("spec") or {}).get("template") or {}).get("spec", {}).get("containers", [])
        for container in containers:
            env_list = container.get("env", [])
            for entry in env_list:
                name = entry.get("name")
                if name in {"TOKENIZE_METRICS_PORT", "METRICS_PORT"}:
                    metrics_port = entry.get("value")
                    break
            if metrics_port:
                break
        actions_pod = self._get_actions_pod()
        if metrics_port and actions_pod:
            metrics_status["enabled"] = True
            command = (
                f"if command -v curl >/dev/null 2>&1; then curl -sf http://127.0.0.1:{metrics_port}/metrics; "
                f"else wget -qO- http://127.0.0.1:{metrics_port}/metrics; fi"
            )
            proc = self._run(
                "kubectl",
                "-n",
                self.namespace,
                "exec",
                actions_pod,
                "--",
                "sh",
                "-lc",
                command,
                capture_output=True,
                check=False,
            )
            if proc.returncode != 0:
                stderr_lower = (proc.stderr or "").lower()
                if proc.returncode == 127 or "not found" in stderr_lower:
                    metrics_status["error"] = "curl-or-wget-missing"
                else:
                    raise VerificationError(
                        "Metrics endpoint unreachable",
                        context={"code": proc.returncode, "stderr": proc.stderr.strip()},
                    )
            metrics_status["sample"] = proc.stdout.splitlines()[:5]
        return {
            "logFiles": collected,
            "actionsLogsValidated": has_run_completed,
            "requestId": request_id_context,
            "metrics": metrics_status,
        }

    def run(self) -> int:
        steps: List[StepResult] = []

        def execute(name: str, fn):
            start = time.monotonic()
            try:
                data = fn()
            except VerificationError as exc:
                duration = time.monotonic() - start
                steps.append(
                    StepResult(
                        name=name,
                        status="failed",
                        duration=duration,
                        detail=str(exc),
                        data=dict(exc.context or {}),
                    )
                )
                return False
            else:
                duration = time.monotonic() - start
                steps.append(
                    StepResult(
                        name=name,
                        status="passed",
                        duration=duration,
                        detail="ok",
                        data=data or {},
                    )
                )
                return True

        execute("cluster-health", self.verify_cluster)
        execute("datahub-readiness", self.verify_datahub)
        execute("postgres-isolation", self.verify_postgres)
        execute("ingested-metadata", self.verify_dataset_metadata)
        execute("tokenization", self.verify_tokenization)
        execute("idempotency", self.verify_idempotency)
        execute("negative-path", self.verify_negative_path)
        execute("observability", self.verify_observability)

        report_path = self.verify_dir / "report.json"
        junit_path = self.verify_dir / "junit.xml"
        self._write_report(report_path, steps)
        self._write_junit(junit_path, steps)

        failed = [step for step in steps if step.status == "failed"]
        for step in steps:
            status_icon = "✅" if step.status == "passed" else "❌"
            print(f"{status_icon} {step.name} ({step.duration:.2f}s): {step.detail}")
        print(f"Artifacts written to {self.verify_dir}")
        return 0 if not failed else 1

    # -------- Artifact writers ---------
    def _write_report(self, path: Path, steps: Sequence[StepResult]) -> None:
        payload = {
            "generatedAt": _dt.datetime.utcnow().isoformat() + "Z",
            "namespace": self.namespace,
            "tenant": self.tenant,
            "datasetUrn": self.dataset_urn,
            "requestId": self.request_id,
            "timeoutSeconds": self.timeout,
            "summary": {
                "total": len(steps),
                "passed": sum(1 for s in steps if s.status == "passed"),
                "failed": sum(1 for s in steps if s.status == "failed"),
            },
            "steps": [step.to_json() for step in steps],
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def _write_junit(self, path: Path, steps: Sequence[StepResult]) -> None:
        import xml.etree.ElementTree as ET

        tests = len(steps)
        failures = sum(1 for step in steps if step.status == "failed")
        root = ET.Element("testsuite", attrib={
            "name": "poc.verify",
            "tests": str(tests),
            "failures": str(failures),
        })
        for step in steps:
            case = ET.SubElement(
                root,
                "testcase",
                attrib={"name": step.name, "time": f"{step.duration:.3f}"},
            )
            if step.status == "failed":
                failure = ET.SubElement(case, "failure", attrib={"message": step.detail})
                if step.data:
                    failure.text = json.dumps(step.data, indent=2, sort_keys=True)
        tree = ET.ElementTree(root)
        ensure_dir(path.parent)
        tree.write(path, encoding="utf-8", xml_declaration=True)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify the POC deployment end-to-end")
    parser.add_argument("--namespace", default=os.environ.get("POC_NAMESPACE", "datahub"))
    parser.add_argument("--tenant", default=DEFAULT_TENANT)
    parser.add_argument("--dataset-urn", default=DEFAULT_DATASET_URN)
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("POC_TIMEOUT", 1200)))
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path(os.environ.get("POC_ARTIFACTS", "artifacts")),
    )
    parser.add_argument(
        "--expect-idempotent",
        action="store_true",
        help="Fail if tokenization re-run causes additional writes",
    )
    parser.add_argument(
        "--request-id",
        default=os.environ.get("POC_REQUEST_ID", DEFAULT_REQUEST_ID),
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    verifier = POCVerifier(
        namespace=args.namespace,
        tenant=args.tenant,
        dataset_urn=args.dataset_urn,
        timeout=args.timeout,
        artifacts_dir=args.artifacts_dir,
        expect_idempotent=args.expect_idempotent,
        request_id=args.request_id,
    )
    return verifier.run()


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    sys.exit(main())
