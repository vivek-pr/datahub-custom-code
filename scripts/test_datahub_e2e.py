#!/usr/bin/env python3
import argparse
import sys
import time

try:
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.mce_builder import make_dataset_urn
    from datahub.metadata.schema_classes import (
        MetadataChangeEventClass as MCE,
        DatasetSnapshotClass,
        DatasetPropertiesClass,
        OwnerClass,
        OwnershipClass,
        OwnershipTypeClass,
        OwnerTypeClass,
        CorpuserUrn,
    )
    from datahub.ingestion.graph.client import DatahubGraph
except Exception:
    print("Missing or incompatible datahub SDK. Did you install scripts/requirements.txt?", file=sys.stderr)
    raise

import os


def resolve_gms_url(namespace: str) -> str:
    env = os.environ.get("DATAHUB_GMS")
    if env:
        return env.rstrip("/")
    import subprocess
    for svc in ("datahub-datahub-gms", "datahub-gms"):
        try:
            out = subprocess.check_output(["minikube", "service", "-n", namespace, svc, "--url"], stderr=subprocess.DEVNULL)
            urls = out.decode().strip().splitlines()
            if urls:
                return urls[0].rstrip("/")
        except Exception:
            continue
    raise RuntimeError("Could not determine GMS URL; set DATAHUB_GMS or ensure service exists in cluster")


def ingest_sample_dataset(gms: str, platform: str, name: str, env: str = "PROD") -> str:
    urn = make_dataset_urn(platform=platform, name=name, env=env)
    snapshot = DatasetSnapshotClass(
        urn=urn,
        aspects=[
            DatasetPropertiesClass(name=name, description="Codex CLI E2E sample dataset"),
            OwnershipClass(owners=[OwnerClass(owner=CorpuserUrn("datahub"), type=OwnershipTypeClass.TECHNICAL_OWNER, ownerType=OwnerTypeClass.CORP_USER)]),
        ],
    )
    mce = MCE(proposedSnapshot=snapshot)
    emitter = DatahubRestEmitter(gms_server=gms)
    emitter.emit_mce(mce)
    emitter.flush()
    return urn


def wait_for_dataset(graph: DatahubGraph, urn: str, timeout: int = 120):
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            res = graph.execute_graphql(
                """
                query($urn: String!) {
                  dataset(urn: $urn) { urn properties { name description } }
                }
                """,
                variables={"urn": urn},
            )
            data = res if isinstance(res, dict) else getattr(res, "data", {})
            ds = data.get("dataset") if isinstance(data, dict) else None
            if ds and ds.get("urn") == urn:
                return ds
        except Exception as e:
            last_err = e
        time.sleep(3)
    raise AssertionError(f"Dataset {urn} not visible via GraphQL within timeout. Last error: {last_err}")


def main():
    parser = argparse.ArgumentParser(description="DataHub E2E smoke: ingest sample dataset and verify it appears")
    parser.add_argument("--namespace", "-n", default="datahub")
    parser.add_argument("--platform", default="bigquery")
    parser.add_argument("--name", default="codex_cli_e2e.sample")
    args = parser.parse_args()

    gms = resolve_gms_url(args.namespace)
    graph = DatahubGraph(gms_server=gms)

    urn = ingest_sample_dataset(gms, args.platform, args.name)
    print(f"Ingested dataset: {urn}")
    ds = wait_for_dataset(graph, urn)
    print("Verified dataset via GraphQL:", ds)
    print("E2E smoke passed.")


if __name__ == "__main__":
    main()
