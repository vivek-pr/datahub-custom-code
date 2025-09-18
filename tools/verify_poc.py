#!/usr/bin/env python3
"""POC verifier for the Minikube DataHub proof-of-concept stack."""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

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
        return {"dataset": dataset.get("urn"), "piiFields": pii_tagged}

    def verify_tokenization(self) -> Dict[str, Any]:
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
            "variables": {"urn": self.dataset_urn, "request": self.request_id},
        }
        response = self._http_post(f"{gms}/api/graphql", json_payload=mutation)
        trigger = ((response.get("data") or {}).get("triggerTokenization") or {})
        request_id = trigger.get("requestId") or self.request_id
        poll_query = {
            "query": """
                query TokenizationRuns($urn: String!, $request: String!) {
                  tokenizationRuns(urn: $urn, requestId: $request) {
                    requestId
                    status
                    rowsAffected
                  }
                }
            """,
            "variables": {"urn": self.dataset_urn, "request": request_id},
        }
        deadline = time.time() + self.timeout
        states: List[str] = []
        rows_affected = None
        while time.time() < deadline:
            data = self._http_post(f"{gms}/api/graphql", json_payload=poll_query)
            runs = ((data.get("data") or {}).get("tokenizationRuns") or [])
            if runs:
                run = runs[0]
                state = run.get("status") or ""
                if state:
                    states.append(state)
                rows_affected = run.get("rowsAffected")
                if state and state.upper() in {"COMPLETED", "SUCCESS", "FAILED", "FAILURE"}:
                    break
            time.sleep(5)
        if not states:
            raise VerificationError(
                "Tokenization run did not report any status",
                context={"runs": []},
            )
        evaluation = evaluate_run_transitions(states, expect_success=True)
        if rows_affected is None:
            raise VerificationError(
                "Tokenization run did not include rowsAffected",
                context={"runs": states},
            )
        if rows_affected <= 0:
            raise VerificationError(
                "Tokenization run did not update any rows",
                context={"rowsAffected": rows_affected},
            )
        return {
            "states": evaluation["states"],
            "final": evaluation["final"],
            "rowsAffected": rows_affected,
            "requestId": request_id,
        }

    def verify_idempotency(self) -> Dict[str, Any]:
        if not self.expect_idempotent:
            return {"skipped": True}
        gms = self._resolve_service_url(
            f"{os.environ.get('RELEASE_DATAHUB', 'datahub')}-datahub-gms",
            "datahub-datahub-gms",
            "datahub-gms",
        )
        query = {
            "query": """
                query TokenizationRuns($urn: String!, $request: String!) {
                  tokenizationRuns(urn: $urn, requestId: $request) {
                    status
                    rowsAffected
                  }
                }
            """,
            "variables": {"urn": self.dataset_urn, "request": self.request_id},
        }
        data = self._http_post(f"{gms}/api/graphql", json_payload=query)
        runs = ((data.get("data") or {}).get("tokenizationRuns") or [])
        if not runs:
            raise VerificationError(
                "No runs recorded for idempotency probe",
                context={"response": data},
            )
        rows = [run.get("rowsAffected", 0) for run in runs]
        if any(r > 0 for r in rows[1:]):
            raise VerificationError(
                "Idempotency probe detected additional writes",
                context={"rowsAffected": rows},
            )
        return {"rowsAffected": rows}

    def verify_negative_path(self) -> Dict[str, Any]:
        gms = self._resolve_service_url(
            f"{os.environ.get('RELEASE_DATAHUB', 'datahub')}-datahub-gms",
            "datahub-datahub-gms",
            "datahub-gms",
        )
        mutation = {
            "query": """
                mutation TriggerFailure($urn: String!, $request: String!) {
                  triggerTokenization(input: {resourceUrn: $urn, requestId: $request, dryRun: true}) {
                    requestId
                    status
                  }
                }
            """,
            "variables": {"urn": self.dataset_urn, "request": f"{self.request_id}-negative"},
        }
        response = self._http_post(f"{gms}/api/graphql", json_payload=mutation)
        trigger = ((response.get("data") or {}).get("triggerTokenization") or {})
        request_id = trigger.get("requestId") or f"{self.request_id}-negative"
        query = {
            "query": """
                query NegativeRuns($urn: String!, $request: String!) {
                  tokenizationRuns(urn: $urn, requestId: $request) {
                    status
                    rowsAffected
                  }
                }
            """,
            "variables": {"urn": self.dataset_urn, "request": request_id},
        }
        deadline = time.time() + min(600, self.timeout)
        states: List[str] = []
        rows: List[int] = []
        while time.time() < deadline:
            data = self._http_post(f"{gms}/api/graphql", json_payload=query)
            runs = ((data.get("data") or {}).get("tokenizationRuns") or [])
            if runs:
                run = runs[0]
                state = run.get("status") or ""
                rows.append(run.get("rowsAffected") or 0)
                if state:
                    states.append(state)
                if state and state.upper() in {"FAILED", "FAILURE", "COMPLETED"}:
                    break
            time.sleep(5)
        if not states:
            raise VerificationError(
                "Negative path did not record states",
                context={"runs": []},
            )
        evaluate_run_transitions(states, expect_success=False)
        if any(val != 0 for val in rows):
            raise VerificationError(
                "Negative path mutated data",
                context={"rowsAffected": rows},
            )
        return {"states": states, "rowsAffected": rows}

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
        return {"logFiles": collected}

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
