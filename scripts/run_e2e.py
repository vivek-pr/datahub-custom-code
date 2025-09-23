#!/usr/bin/env python3
"""Execute the end-to-end smoke test flow against the deployed action."""

from __future__ import annotations

import argparse
import base64
import json
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Dict, List, Tuple

PG_DATASET = "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.schema.customers,PROD)"
DBX_DATASET = "urn:li:dataset:(urn:li:dataPlatform:databricks,tokenize.schema.customers,PROD)"
DEFAULT_COLUMNS = ["email", "phone"]


def run(cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, text=True, capture_output=True, **kwargs)


def secret_has_dbx(namespace: str) -> bool:
    try:
        result = run(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "secret",
                "tokenize-poc-secrets",
                "-o",
                "jsonpath={.data.DBX_JDBC_URL}",
            ]
        )
    except subprocess.CalledProcessError:
        return False
    raw = result.stdout.strip()
    if not raw:
        return False
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
    except Exception:
        return False
    return bool(decoded.strip())


def start_port_forward(namespace: str, local_port: int, remote_port: int) -> Tuple[subprocess.Popen[str], threading.Thread, List[str]]:
    cmd = [
        "kubectl",
        "-n",
        namespace,
        "port-forward",
        "svc/tokenize-poc-action",
        f"{local_port}:{remote_port}",
    ]
    output: List[str] = []
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            output.append(line.rstrip())

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()

    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"port-forward exited early: {' | '.join(output)}")
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                return proc, thread, output
        except OSError:
            time.sleep(0.5)
    raise TimeoutError("Timed out waiting for port-forward to become ready")


def stop_port_forward(proc: subprocess.Popen[str], thread: threading.Thread) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    thread.join(timeout=5)


def call_trigger(base_url: str, dataset: str, columns: List[str], limit: int) -> Dict[str, int]:
    payload = json.dumps({"dataset": dataset, "columns": columns, "limit": limit}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/trigger",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        details = error.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Trigger failed ({error.code}): {details}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"Trigger request failed: {error.reason}") from error
    return json.loads(body)


def run_dataset_flow(name: str, base_url: str, dataset: str, limit: int) -> None:
    print(f"\n=== {name} tokenization ===")
    first = call_trigger(base_url, dataset, DEFAULT_COLUMNS, limit)
    print("First run:", json.dumps(first))
    if first.get("updated_count", 0) <= 0:
        raise RuntimeError(f"Expected {name} first run to update rows, saw {first}")
    second = call_trigger(base_url, dataset, DEFAULT_COLUMNS, limit)
    print("Second run:", json.dumps(second))
    if second.get("updated_count") != 0:
        raise RuntimeError(f"Expected {name} second run to update zero rows, saw {second}")



def main() -> int:
    parser = argparse.ArgumentParser(description="Run the tokenization smoke tests")
    parser.add_argument("--namespace", default="tokenize-poc", help="Kubernetes namespace")
    parser.add_argument("--limit", type=int, default=100, help="Row limit per trigger")
    parser.add_argument("--local-port", type=int, default=18080, help="Local port for port-forward")
    parser.add_argument("--remote-port", type=int, default=8080, help="Service port")
    args = parser.parse_args()

    base_url = f"http://127.0.0.1:{args.local_port}"

    try:
        proc, thread, logs = start_port_forward(args.namespace, args.local_port, args.remote_port)
    except Exception as error:  # pragma: no cover - diagnostic
        raise SystemExit(f"Failed to establish port-forward: {error}")

    try:
        run_dataset_flow("Postgres", base_url, PG_DATASET, args.limit)
        if secret_has_dbx(args.namespace):
            print("Databricks secret detected; running Databricks flow")
            run_dataset_flow("Databricks", base_url, DBX_DATASET, args.limit)
        else:
            print("DBX_JDBC_URL not configured; skipping Databricks flow")
    finally:
        stop_port_forward(proc, thread)

    print("\nSmoke test completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
