"""Unit tests for the PII detector heuristics."""

from __future__ import annotations

from typing import Dict, Sequence

from action.models import DatasetMetadata, DatasetRef, FieldMetadata
from action.pii_detector import PiiDetector

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.public.customers,PROD)"
)


def build_dataset(fields: Sequence[FieldMetadata]) -> DatasetMetadata:
    return DatasetMetadata(
        urn=DATASET_URN,
        ref=DatasetRef.from_urn(DATASET_URN),
        name="customers",
        platform="postgres",
        global_tags=set(),
        fields=list(fields),
        editable_properties={},
    )


def field(field_path: str, tags: Sequence[str] | None = None) -> FieldMetadata:
    return FieldMetadata(field_path=field_path, tags=set(tags or []))


def test_tag_driven_detection_prefers_existing_tags() -> None:
    dataset = build_dataset(
        [
            field("customers.email", tags=["urn:li:tag:pii.email"]),
            field("customers.phone", tags=["urn:li:tag:something"]),
            field("customers.notes", tags=[]),
        ]
    )
    detector = PiiDetector.from_env()
    columns = detector.detect(dataset)
    assert columns == {"email"}


def test_heuristic_detection_uses_patterns_and_samples() -> None:
    dataset = build_dataset(
        [
            field("customers.customer_email"),
            field("customers.primary_phone"),
            field("customers.city"),
        ]
    )
    detector = PiiDetector.from_env()
    samples: Dict[str, Sequence[str]] = {
        "customer_email": ["alice@example.com", "bob@example.com"],
        "primary_phone": ["not-a-phone"],
    }
    columns = detector.detect(dataset, samples=samples)
    assert columns == {"customer_email"}


def test_explicit_field_scope_is_respected() -> None:
    dataset = build_dataset(
        [
            field("customers.email"),
            field("customers.phone"),
        ]
    )
    detector = PiiDetector.from_env()
    columns = detector.detect(dataset, explicit_fields=["customers.phone"])
    assert columns == {"phone"}
