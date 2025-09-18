#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import Optional

import requests

try:
    from datahub.ingestion.graph.client import DatahubGraph
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.mce_builder import make_tag_urn
    from datahub.metadata.schema_classes import TagPropertiesClass
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
except Exception as e:
    print("Missing or incompatible datahub SDK. Did you install scripts/requirements.txt?", file=sys.stderr)
    raise


def kubectl_service_url(ns: str, name: str) -> Optional[str]:
    import subprocess
    try:
        out = subprocess.check_output(["minikube", "service", "-n", ns, name, "--url"], stderr=subprocess.DEVNULL)
        urls = out.decode().strip().splitlines()
        return urls[0] if urls else None
    except Exception:
        return None


def resolve_gms_url(namespace: str) -> str:
    # Priority: env var DATAHUB_GMS -> minikube service lookup (release or default)
    env = os.environ.get("DATAHUB_GMS")
    if env:
        return env.rstrip("/")
    # Try common service names
    for svc in ("datahub-datahub-gms", "datahub-gms"):
        url = kubectl_service_url(namespace, svc)
        if url:
            return url.rstrip("/")
    raise RuntimeError("Could not determine GMS URL; set DATAHUB_GMS or ensure service exists in cluster")


def assert_http_200(url: str, timeout: int = 60):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            last = resp.status_code
            if resp.status_code == 200:
                return
        except Exception as e:
            last = str(e)
        time.sleep(2)
    raise AssertionError(f"Timeout waiting for HTTP 200 from {url} (last: {last})")


def check_graphql_200(gms_url: str, timeout: int = 60):
    deadline = time.time() + timeout
    q = {"query": "query { __typename }"}
    last = None
    while time.time() < deadline:
        try:
            resp = requests.post(f"{gms_url}/api/graphql", json=q, timeout=5)
            last = resp.status_code
            if resp.status_code == 200:
                return
        except Exception as e:
            last = str(e)
        time.sleep(2)
    raise AssertionError(f"Timeout waiting for GraphQL 200 at {gms_url}/api/graphql (last: {last})")


def upsert_and_fetch_tag(gms_url: str, tag_name: str, timeout: int = 60):
    # Upsert via REST emitter
    emitter = DatahubRestEmitter(gms_server=gms_url)
    urn = make_tag_urn(tag_name)
    mcp = MetadataChangeProposalWrapper(
        entityType="tag",
        entityUrn=urn,
        aspect=TagPropertiesClass(name=tag_name, description="Codex CLI integration test tag"),
    )
    emitter.emit(mcp)
    emitter.flush()

    # Fetch via GraphQL
    graph = DatahubGraph(gms_server=gms_url)
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            res = graph.execute_graphql(
                """
                query($urn: String!) {
                  tag(urn: $urn) {
                    urn
                    name
                    properties { name description }
                  }
                }
                """,
                variables={"urn": urn},
            )
            tag = res.get("tag") if isinstance(res, dict) else (res.data.get("tag") if hasattr(res, "data") else None)
            if tag and tag.get("name") == tag_name:
                return tag
        except Exception as e:
            last_err = e
        time.sleep(2)
    raise AssertionError(f"Tag {urn} not retrievable via GraphQL within timeout. Last error: {last_err}")


def main():
    parser = argparse.ArgumentParser(description="DataHub integration check: GraphQL + REST emitter tag upsert")
    parser.add_argument("--namespace", "-n", default="datahub")
    parser.add_argument("--tag", default="codex_cli_dummy")
    args = parser.parse_args()

    gms = resolve_gms_url(args.namespace)
    print(f"Using GMS: {gms}")

    print("Asserting /api/graphiql returns 200...")
    assert_http_200(f"{gms}/api/graphiql")
    print("OK")

    print("Asserting /api/graphql returns 200...")
    check_graphql_200(gms)
    print("OK")

    print(f"Upserting and fetching tag '{args.tag}' via REST emitter + GraphQL...")
    tag = upsert_and_fetch_tag(gms, args.tag)
    print("OK ->", json.dumps(tag))

    print("Integration checks passed.")


if __name__ == "__main__":
    main()
