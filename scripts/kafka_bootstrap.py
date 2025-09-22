"""Kafka bootstrap and health check utilities for the PoC stack."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Iterable

import requests
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

LOGGER = logging.getLogger("kafka_bootstrap")

REPO_ROOT = Path(__file__).resolve().parents[1]
AVRO_DIR = REPO_ROOT / "init" / "avro"

REQUIRED_TOPICS: dict[str, dict[str, object]] = {
    "MetadataChangeEvent_v4": {
        "partitions": 1,
        "replication_factor": 1,
        "schema_file": "MetadataChangeEvent.avsc",
    },
    "FailedMetadataChangeEvent_v4": {
        "partitions": 1,
        "replication_factor": 1,
        "schema_file": "FailedMetadataChangeEvent.avsc",
    },
    "MetadataAuditEvent_v4": {
        "partitions": 1,
        "replication_factor": 1,
        "schema_file": "MetadataAuditEvent.avsc",
    },
    "FailedMetadataAuditEvent_v4": {
        "partitions": 1,
        "replication_factor": 1,
        "schema_file": "MetadataAuditEvent.avsc",
    },
    "DataHubUsageEvent_v1": {
        "partitions": 1,
        "replication_factor": 1,
        # Usage events are emitted as UTF-8 JSON strings, so we register a simple string schema.
        "schema_literal": '"string"',
    },
    "PlatformEvent_v1": {
        "partitions": 1,
        "replication_factor": 1,
        "schema_file": "PlatformEvent.avsc",
    },
}


def _load_schema(topic_name: str) -> str | None:
    cfg = REQUIRED_TOPICS[topic_name]
    literal = cfg.get("schema_literal")
    if literal is not None:
        return str(literal)
    schema_file = cfg.get("schema_file")
    if not schema_file:
        return None
    schema_path = AVRO_DIR / str(schema_file)
    if not schema_path.exists():
        raise FileNotFoundError(f"Missing schema file for {subject}: {schema_path}")
    return schema_path.read_text(encoding="utf-8")


def _topic_subject(topic_name: str) -> str:
    return f"{topic_name}-value"


def _ensure_topics(admin: KafkaAdminClient) -> set[str]:
    existing = set(admin.list_topics())
    to_create: list[NewTopic] = []
    for topic, cfg in REQUIRED_TOPICS.items():
        partitions = int(cfg.get("partitions", 1))
        replication = int(cfg.get("replication_factor", 1))
        if topic in existing:
            LOGGER.info("Topic %s already exists", topic)
            continue
        LOGGER.info(
            "Creating topic %s with partitions=%s replication=%s",
            topic,
            partitions,
            replication,
        )
        to_create.append(
            NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=replication,
            )
        )
    if to_create:
        try:
            admin.create_topics(new_topics=to_create, validate_only=False)
            LOGGER.info("Created %s topic(s)", len(to_create))
        except TopicAlreadyExistsError:
            LOGGER.info("Topic creation reported existing topics; continuing")
    return set(admin.list_topics())


def _describe_topics(admin: KafkaAdminClient, topics: Iterable[str]) -> None:
    resources = [
        ConfigResource(ConfigResourceType.TOPIC, topic, configs=[]) for topic in topics
    ]
    try:
        configs = admin.describe_configs(resources)
    except UnknownTopicOrPartitionError as exc:
        LOGGER.warning("Failed to describe configs: %s", exc)
        return
    for resource, entries in configs.items():
        topic = resource.name
        interesting = {
            entry.name: entry.value
            for entry in entries
            if entry.name in {"cleanup.policy", "retention.ms", "min.insync.replicas"}
        }
        LOGGER.info("Topic %s configs: %s", topic, interesting)


def _schemas_match(existing: str, desired: str) -> bool:
    try:
        existing_obj = json.loads(existing)
        desired_obj = json.loads(desired)
    except json.JSONDecodeError:
        return existing.strip() == desired.strip()
    return existing_obj == desired_obj


def _ensure_schema_registered(topic: str, schema_registry_url: str) -> None:
    subject = _topic_subject(topic)
    schema = _load_schema(topic)
    if schema is None:
        LOGGER.info("No schema configured for %s; skipping registration", subject)
        return
    subject_path = f"{schema_registry_url.rstrip('/')}/subjects/{subject}"
    latest = requests.get(f"{subject_path}/versions/latest", timeout=10)
    if latest.status_code == 200:
        existing_schema = latest.json().get("schema", "")
        if _schemas_match(existing_schema, schema):
            LOGGER.info("Schema for %s already registered", subject)
            return
        raise RuntimeError(
            f"Schema registry subject {subject} exists but does not match expected schema"
        )
    if latest.status_code not in (404, 422):
        latest.raise_for_status()
    LOGGER.info("Registering schema for %s", subject)
    response = requests.post(
        f"{subject_path}/versions",
        json={"schema": schema, "schemaType": "AVRO"},
        timeout=10,
    )
    response.raise_for_status()
    LOGGER.info("Registered schema id %s for %s", response.json().get("id"), subject)


def bootstrap(kafka_bootstrap: str, schema_registry_url: str) -> None:
    LOGGER.info("Ensuring Kafka topics exist")
    with KafkaAdminClient(
        bootstrap_servers=kafka_bootstrap,
        client_id="datahub-kafka-bootstrap",
        request_timeout_ms=10_000,
        retry_backoff_ms=500,
    ) as admin:
        topics = _ensure_topics(admin)
        _describe_topics(admin, REQUIRED_TOPICS.keys())
    LOGGER.info("Registering schemas with %s", schema_registry_url)
    for topic in REQUIRED_TOPICS:
        _ensure_schema_registered(topic, schema_registry_url)


def check(kafka_bootstrap: str, schema_registry_url: str) -> None:
    LOGGER.info("Running Kafka auto-heal check")
    with KafkaAdminClient(
        bootstrap_servers=kafka_bootstrap,
        client_id="datahub-kafka-check",
        request_timeout_ms=10_000,
        retry_backoff_ms=500,
    ) as admin:
        topics = _ensure_topics(admin)
        missing = sorted(set(REQUIRED_TOPICS) - topics)
        if missing:
            raise RuntimeError(f"Missing required topics even after creation attempt: {missing}")
        _describe_topics(admin, REQUIRED_TOPICS.keys())
    LOGGER.info("Verifying schema registry subjects")
    for topic in REQUIRED_TOPICS:
        subject = _topic_subject(topic)
        schema = _load_schema(topic)
        if schema is None:
            continue
        subject_path = f"{schema_registry_url.rstrip('/')}/subjects/{subject}"
        latest = requests.get(f"{subject_path}/versions/latest", timeout=10)
        if latest.status_code != 200:
            raise RuntimeError(
                f"Schema registry subject {subject} is missing (status {latest.status_code})"
            )
    LOGGER.info("Kafka auto-heal check succeeded")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap and validate Kafka topics")
    parser.add_argument(
        "mode",
        choices=["bootstrap", "check"],
        help="bootstrap: create topics and register schemas; check: validate readiness",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--schema-registry",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
        help="Schema registry base URL",
    )
    parser.add_argument(
        "--retry", type=int, default=5, help="Number of retries for bootstrap operations"
    )
    parser.add_argument(
        "--sleep", type=int, default=5, help="Seconds to sleep between retries"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv or sys.argv[1:])
    attempts = 0
    while True:
        attempts += 1
        try:
            if args.mode == "bootstrap":
                bootstrap(args.kafka_bootstrap, args.schema_registry)
            else:
                check(args.kafka_bootstrap, args.schema_registry)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error("Attempt %s/%s failed: %s", attempts, args.retry, exc)
            if attempts >= args.retry:
                return 1
            time.sleep(args.sleep)


if __name__ == "__main__":
    sys.exit(main())
