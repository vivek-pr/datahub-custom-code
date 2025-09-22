import io
import os
import struct
import sys
import subprocess
import time
from pathlib import Path

import pytest
import requests
from avro.io import BinaryEncoder, DatumWriter
from avro.schema import parse as parse_avro_schema

import importlib.util

spec = importlib.util.find_spec("kafka")
if not spec or not spec.submodule_search_locations:
    raise ImportError("kafka package not found on sys.path")

vendor_dir = Path(spec.submodule_search_locations[0]) / "vendor"

vendor_spec = importlib.util.spec_from_file_location(
    "kafka.vendor", vendor_dir / "__init__.py"
)
vendor_module = importlib.util.module_from_spec(vendor_spec)
assert vendor_spec.loader is not None
vendor_spec.loader.exec_module(vendor_module)
sys.modules["kafka.vendor"] = vendor_module

six_spec = importlib.util.spec_from_file_location("kafka.vendor.six", vendor_dir / "six.py")
six_module = importlib.util.module_from_spec(six_spec)
assert six_spec.loader is not None
six_spec.loader.exec_module(six_module)
sys.modules["kafka.vendor.six"] = six_module
sys.modules["kafka.vendor.six.moves"] = six_module.moves

from kafka import KafkaProducer

from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
GMS_GRAPHQL = os.getenv("DATAHUB_GMS_GRAPHQL", "http://localhost:8080/api/graphql")
REQUIRED_TOPICS = {
    "MetadataChangeEvent_v4",
    "FailedMetadataChangeEvent_v4",
    "MetadataAuditEvent_v4",
    "FailedMetadataAuditEvent_v4",
    "DataHubUsageEvent_v1",
    "PlatformEvent_v1",
}
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bootstrap,healthy_dataset,PROD)"


@pytest.mark.integration
def test_kafka_topics_bootstrap_and_consumers():
    topics_output = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/bitnami/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--list",
        ],
        check=True,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout
    for expected in REQUIRED_TOPICS:
        assert expected in topics_output

    subject = "MetadataChangeEvent_v4-value"
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects/{subject}/versions/latest",
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    schema_id = int(payload["id"])
    schema_str = payload["schema"]
    parsed_schema = parse_avro_schema(schema_str)

    snapshot = DatasetSnapshotClass(
        urn=DATASET_URN,
        aspects=[DatasetPropertiesClass(description="Kafka bootstrap integration test")],
    )
    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
    avro_ready = mce.to_avro_writable()

    buffer = io.BytesIO()
    writer = DatumWriter(parsed_schema)
    encoder = BinaryEncoder(buffer)
    writer.write(avro_ready, encoder)
    record_bytes = buffer.getvalue()
    message = b"\x00" + struct.pack(">I", schema_id) + record_bytes

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        retries=5,
        linger_ms=10,
        request_timeout_ms=10_000,
    )
    future = producer.send(
        "MetadataChangeEvent_v4",
        key=DATASET_URN.encode("utf-8"),
        value=message,
    )
    future.get(timeout=30)
    producer.flush()
    producer.close()

    query = """
    query Dataset($urn: String!) {
      dataset(urn: $urn) {
        urn
        properties {
          description
        }
      }
    }
    """
    deadline = time.time() + 45
    dataset_doc = None
    while time.time() < deadline:
        gql_response = requests.post(
            GMS_GRAPHQL,
            json={"query": query, "variables": {"urn": DATASET_URN}},
            timeout=10,
        )
        gql_response.raise_for_status()
        data = gql_response.json()
        dataset_doc = data.get("data", {}).get("dataset")
        if dataset_doc:
            break
        time.sleep(2)
    assert dataset_doc is not None, f"Dataset {DATASET_URN} not materialized from Kafka"
    assert dataset_doc["urn"] == DATASET_URN
    assert dataset_doc["properties"]["description"] == "Kafka bootstrap integration test"

    subprocess.run(
        ["docker", "compose", "run", "--rm", "ingestion"],
        check=True,
        cwd=REPO_ROOT,
    )

    for service in ("datahub-gms", "datahub-mce-consumer", "datahub-mae-consumer"):
        logs = subprocess.run(
            ["docker", "compose", "logs", "--tail", "200", service],
            check=True,
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout
        assert "UNKNOWN_TOPIC_OR_PARTITION" not in logs
