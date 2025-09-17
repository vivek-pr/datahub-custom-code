from __future__ import annotations

from typing import List

import pytest

from pii_classifier.classifier import PIIClassifier
from pii_classifier.config import ClassifierConfig, DataHubConfig, PostgresConfig
from pii_classifier.emitter import TagUpsertResult
from pii_classifier.postgres_sampler import ColumnMetadata


class DummySampler:
    def __init__(self, config: PostgresConfig):
        self.config = config

    def __enter__(self) -> "DummySampler":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def iter_columns(self):
        yield ColumnMetadata(schema="t001", table="customers", name="email", data_type="text")
        yield ColumnMetadata(schema="t001", table="customers", name="status", data_type="text")

    def sample_values(self, column: ColumnMetadata, limit: int) -> List[str]:
        if column.name == "email":
            return [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "dana@example.com",
                "eve@example.com",
            ]
        if column.name == "status":
            return ["CAPTURED", "FAILED", "PENDING", "SETTLED", "PAID"]
        return []


class DummyEmitter:
    def __init__(self, config: DataHubConfig):
        self.config = config
        self.seen_specs = None
        self.calls: List[TagUpsertResult] = []

    def ensure_tag_definitions(self, specs):
        self.seen_specs = list(specs)

    def add_field_tag(self, dataset_name, field, tag_urn, confidence, rule_name, reason):
        result = TagUpsertResult(
            schema_field_urn=f"urn:li:schemaField:({dataset_name},{field})",
            tag_urn=tag_urn,
            was_emitted=True,
            confidence=confidence,
            rule_name=rule_name,
            reason=reason,
        )
        self.calls.append(result)
        return result


@pytest.fixture
def classifier_config(tmp_path) -> ClassifierConfig:
    postgres = PostgresConfig(
        host="db",
        port=5432,
        database="sandbox",
        user="user",
        password="pass",
        schemas=["t001"],
        sample_limit=10,
    )
    datahub = DataHubConfig(
        gms="http://localhost:8080",
        token=None,
        platform="postgres",
        env="PROD",
        dry_run=True,
    )
    rules_path = "sample/regex/rules.yml"
    return ClassifierConfig(postgres=postgres, datahub=datahub, rules_path=rules_path, min_value_samples=3)


def test_classifier_applies_tags(monkeypatch, classifier_config):
    dummy_emitter = DummyEmitter(classifier_config.datahub)
    monkeypatch.setattr("pii_classifier.classifier.PostgresSampler", DummySampler)
    monkeypatch.setattr("pii_classifier.classifier.DataHubTagEmitter", lambda cfg: dummy_emitter)

    classifier = PIIClassifier(classifier_config)
    matches = classifier.run()

    assert dummy_emitter.seen_specs is not None
    assert any(call.tag_urn == "urn:li:tag:pii-email" for call in dummy_emitter.calls)
    assert matches
    assert matches[0].evaluation.rule.name == "pii_email"


def test_classifier_skips_non_matches(monkeypatch, classifier_config):
    class EmptySampler(DummySampler):
        def iter_columns(self):
            yield ColumnMetadata(schema="t001", table="customers", name="status", data_type="text")

    dummy_emitter = DummyEmitter(classifier_config.datahub)
    monkeypatch.setattr("pii_classifier.classifier.PostgresSampler", EmptySampler)
    monkeypatch.setattr("pii_classifier.classifier.DataHubTagEmitter", lambda cfg: dummy_emitter)

    classifier = PIIClassifier(classifier_config)
    matches = classifier.run()

    assert not matches
    assert not dummy_emitter.calls
