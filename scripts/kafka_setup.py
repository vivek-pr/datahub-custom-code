"""Idempotent Kafka topic bootstrap for the DataHub PoC stack."""

from __future__ import annotations

import argparse
import logging
import os
import random
import subprocess
import sys
import time
from typing import Sequence

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import (
    LeaderNotAvailableError,
    NoBrokersAvailable,
    NotControllerError,
    TopicAlreadyExistsError,
)

LOGGER = logging.getLogger("kafka_setup")

REQUIRED_TOPICS: Sequence[str] = (
    "MetadataChangeEvent_v4",
    "FailedMetadataChangeEvent_v4",
    "MetadataAuditEvent_v4",
    "FailedMetadataAuditEvent_v4",
    "DataHubUsageEvent_v1",
    "PlatformEvent_v1",
)

DEFAULT_RETENTION_MS = "604800000"  # seven days
MAX_ATTEMPTS = 8
RETRYABLE_ERRORS = (
    LeaderNotAvailableError,
    NoBrokersAvailable,
    NotControllerError,
)


def _bool_env(var: str, default: bool = False) -> bool:
    value = os.environ.get(var)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_api_version(raw: str | None) -> tuple[int, ...] | None:
    if not raw:
        return None
    try:
        return tuple(int(part) for part in raw.split("."))
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Invalid API_VERSION value: {raw!r}") from exc


def _extra_topics(raw: str | None) -> Sequence[str]:
    if not raw:
        return ()
    return tuple(topic.strip() for topic in raw.split(",") if topic.strip())


def _call_with_retries(description: str, func):
    delay = 1.0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return func()
        except TopicAlreadyExistsError:
            LOGGER.info("%s: topics already exist", description)
            return None
        except RETRYABLE_ERRORS as exc:
            if attempt >= MAX_ATTEMPTS:
                LOGGER.error("%s failed after %s attempts: %s", description, attempt, exc)
                raise
            sleep_for = min(delay, 30.0) + random.uniform(0.0, delay / 2.0)
            LOGGER.warning(
                "%s attempt %s/%s failed: %s; retrying in %.1fs",
                description,
                attempt,
                MAX_ATTEMPTS,
                exc,
                sleep_for,
            )
            time.sleep(sleep_for)
            delay *= 2


def _log_cluster_metadata(admin: KafkaAdminClient) -> None:
    try:
        version = admin._client.check_version()  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - best-effort logging
        LOGGER.debug("Unable to determine broker version: %s", exc)
    else:
        LOGGER.info("Connected to Kafka broker version %s", version)
    try:
        cluster = admin.describe_cluster()
    except Exception as exc:  # pragma: no cover - best-effort logging
        LOGGER.debug("Unable to describe cluster: %s", exc)
        return
    LOGGER.info(
        "Cluster metadata: id=%s, controller_id=%s, nodes=%s",
        cluster.get("cluster_id"),
        cluster.get("controller_id"),
        cluster.get("brokers"),
    )


def _list_topics_with_retries(admin: KafkaAdminClient) -> set[str]:
    def _list() -> set[str]:
        topics = set(admin.list_topics())
        LOGGER.debug("Broker returned %s topic(s)", len(topics))
        return topics

    topics = _call_with_retries("list topics", _list)
    return topics or set()


def _ensure_topics(
    admin: KafkaAdminClient,
    topics: Sequence[str],
    partitions: int,
    replication_factor: int,
    retention_ms: str,
) -> set[str]:
    existing = _list_topics_with_retries(admin)
    if existing:
        LOGGER.info("Existing topics: %s", ", ".join(sorted(existing)))
    else:
        LOGGER.info("No topics reported yet")
    to_create = [
        NewTopic(
            name=name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            topic_configs={
                "cleanup.policy": "delete",
                "retention.ms": retention_ms,
            },
        )
        for name in topics
        if name not in existing
    ]
    if to_create:
        LOGGER.info(
            "Creating %s topic(s): %s",
            len(to_create),
            ", ".join(topic.name for topic in to_create),
        )
        _call_with_retries(
            "create topics",
            lambda: admin.create_topics(
                new_topics=to_create,
                validate_only=False,
                timeout_ms=30_000,
            ),
        )
        existing = _list_topics_with_retries(admin)
    else:
        LOGGER.info("All required topics already exist")
    return existing


def _run_datahub_cli() -> bool:
    if not _bool_env("DATAHUB_CLI_ENABLED"):
        return False
    try:
        LOGGER.info("Attempting topic creation via DataHub CLI")
        subprocess.run(
            ["datahub", "kafka-setup", "--no-verify"],
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        LOGGER.info("DataHub CLI kafka-setup completed successfully")
        return True
    except FileNotFoundError as exc:
        LOGGER.warning("DataHub CLI not available: %s", exc)
    except subprocess.CalledProcessError as exc:
        LOGGER.warning("DataHub CLI kafka-setup failed (code %s); falling back", exc.returncode)
    return False


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ensure DataHub Kafka topics exist")
    parser.add_argument(
        "--partitions",
        type=int,
        default=int(os.environ.get("PARTITIONS", "1")),
        help="Partitions per topic (default: %(default)s or PARTITIONS env)",
    )
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=int(os.environ.get("REPLICATION_FACTOR", "1")),
        help="Replication factor per topic (default: %(default)s or REPLICATION_FACTOR env)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap servers (default: %(default)s or KAFKA_BOOTSTRAP_SERVERS env)",
    )
    parser.add_argument(
        "--api-version",
        default=os.environ.get("API_VERSION", "2.5.0"),
        help="Kafka broker API version (default: %(default)s or API_VERSION env)",
    )
    parser.add_argument(
        "--retention-ms",
        default=os.environ.get("RETENTION_MS", DEFAULT_RETENTION_MS),
        help="Topic retention in milliseconds (default: %(default)s or RETENTION_MS env)",
    )
    parser.add_argument(
        "--extra-topics",
        default=os.environ.get("EXTRA_TOPICS", ""),
        help="Comma separated list of extra topics to create",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level (default: %(default)s or LOG_LEVEL env)",
    )
    return parser.parse_args(argv)


def ensure_topics(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv or sys.argv[1:])
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    required_topics = tuple(dict.fromkeys([*REQUIRED_TOPICS, *_extra_topics(args.extra_topics)]))
    LOGGER.info("Ensuring topics exist: %s", ", ".join(required_topics))

    _run_datahub_cli()

    api_version = _parse_api_version(args.api_version)
    LOGGER.info(
        "Connecting to Kafka at %s (client_id=kafka-setup, api_version=%s)",
        args.bootstrap_servers,
        api_version or "auto",
    )

    def _connect() -> KafkaAdminClient:
        return KafkaAdminClient(
            bootstrap_servers=args.bootstrap_servers,
            client_id="kafka-setup",
            api_version=api_version,
            request_timeout_ms=10_000,
        )

    admin = _call_with_retries("connect to kafka", _connect)
    if admin is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Unable to connect to Kafka after retries")
    try:
        _log_cluster_metadata(admin)
        existing = _ensure_topics(
            admin,
            required_topics,
            args.partitions,
            args.replication_factor,
            str(args.retention_ms),
        )
    finally:
        admin.close()

    missing = sorted(set(required_topics) - existing)
    if missing:
        raise RuntimeError(f"Missing topics after creation attempt: {missing}")
    LOGGER.info("Kafka topic bootstrap complete. Topics ready: %s", ", ".join(required_topics))


def main(argv: Sequence[str] | None = None) -> int:
    try:
        ensure_topics(argv)
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.error("Kafka setup failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
