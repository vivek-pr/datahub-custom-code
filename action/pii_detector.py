"""PII detection logic used by the tokenization action."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence, Set

import yaml

from .models import DatasetMetadata, FieldMetadata

LOGGER = logging.getLogger(__name__)

DEFAULT_PATTERN_CONFIG = {
    "name_patterns": [
        "email",
        "e_mail",
        "phone",
        "mobile",
        "contact",
        "ssn",
        "aadhaar",
    ],
    "regex_validators": {
        "email": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        "phone": r"^[+0-9()\-\s]{7,}$",
    },
}

PII_TAG_SUFFIXES = {
    "pii",
    "pii.email",
    "pii.phone",
    "pii.contact",
    "sensitive",
}


@dataclass
class DetectorConfig:
    name_patterns: Sequence[str]
    regex_validators: Dict[str, str]

    @classmethod
    def from_dict(cls, payload: Dict[str, object]) -> "DetectorConfig":
        patterns = (
            payload.get("name_patterns") or DEFAULT_PATTERN_CONFIG["name_patterns"]
        )
        validators = (
            payload.get("regex_validators")
            or DEFAULT_PATTERN_CONFIG["regex_validators"]
        )
        return cls(list(patterns), dict(validators))


class PiiDetector:
    """Identify columns that should be tokenised."""

    def __init__(self, config: DetectorConfig) -> None:
        self.config = config
        self._compiled_validators = {
            key: re.compile(pattern)
            for key, pattern in self.config.regex_validators.items()
        }

    @classmethod
    def from_env(cls) -> "PiiDetector":
        path = os.environ.get("PII_CONFIG_PATH")
        if path:
            try:
                with Path(path).expanduser().open("r", encoding="utf-8") as handle:
                    data = yaml.safe_load(handle) or {}
                LOGGER.info("Loaded PII detector config from %s", path)
            except FileNotFoundError:
                LOGGER.warning("PII_CONFIG_PATH %s not found; using defaults", path)
                data = DEFAULT_PATTERN_CONFIG
            except Exception as exc:  # pragma: no cover - configuration errors
                LOGGER.warning(
                    "Failed to parse %s (%s); falling back to defaults", path, exc
                )
                data = DEFAULT_PATTERN_CONFIG
        else:
            data = DEFAULT_PATTERN_CONFIG
        return cls(DetectorConfig.from_dict(data))

    # ------------------------------------------------------------------
    def detect(
        self,
        dataset: DatasetMetadata,
        *,
        explicit_fields: Optional[Iterable[str]] = None,
        samples: Optional[Dict[str, Sequence[str]]] = None,
    ) -> Set[str]:
        """Return column names that should be tokenised."""

        if explicit_fields:
            columns = {
                self._normalise_field(dataset, field_name)
                for field_name in explicit_fields
            }
            return {column for column in columns if column}

        tagged = {field.column for field in dataset.fields if self._has_pii_tag(field)}
        if tagged:
            return tagged

        return self._heuristic_detection(dataset.fields, samples or {})

    # ------------------------------------------------------------------
    def _normalise_field(
        self, dataset: DatasetMetadata, field_name: str
    ) -> Optional[str]:
        for field in dataset.fields:
            if field.field_path == field_name or field.column == field_name:
                return field.column
        LOGGER.debug(
            "Unknown field referenced for dataset %s: %s", dataset.urn, field_name
        )
        return None

    def _has_pii_tag(self, field: FieldMetadata) -> bool:
        suffixes = tuple(PII_TAG_SUFFIXES)
        for tag in field.tags:
            if tag.endswith(suffixes):
                return True
            if tag.startswith("urn:li:tag:") and tag.split(":")[-1] in PII_TAG_SUFFIXES:
                return True
        return False

    def _heuristic_detection(
        self,
        fields: Sequence[FieldMetadata],
        samples: Dict[str, Sequence[str]],
    ) -> Set[str]:
        patterns = [pattern.lower() for pattern in self.config.name_patterns]
        detected: Set[str] = set()
        for field in fields:
            column = field.column
            lowered = column.lower()
            for pattern in patterns:
                if pattern in lowered:
                    if self._validate_with_samples(pattern, column, samples):
                        detected.add(column)
                        break
        return detected

    def _validate_with_samples(
        self,
        pattern: str,
        column: str,
        samples: Dict[str, Sequence[str]],
    ) -> bool:
        validator = self._compiled_validators.get(pattern)
        if not validator:
            return True
        values = samples.get(column)
        if not values:
            return True
        for value in values:
            if value is None:
                continue
            if validator.match(str(value)):
                return True
        LOGGER.debug(
            "Discarding column %s; samples failed %s validation", column, pattern
        )
        return False

    def describe(self) -> Dict[str, object]:
        return {
            "name_patterns": list(self.config.name_patterns),
            "regex_validators": list(self.config.regex_validators.keys()),
        }


def as_json(detector: PiiDetector) -> str:
    return json.dumps(detector.describe())
