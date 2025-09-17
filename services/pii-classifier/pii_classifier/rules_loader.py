"""Load and evaluate regex rules for PII detection."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import yaml


@dataclass
class Rule:
    name: str
    tag: str
    name_patterns: List[re.Pattern]
    value_pattern: Optional[re.Pattern]
    min_confidence: float
    name_weight: float
    value_weight: float
    value_match_ratio: float
    description: Optional[str] = None

    def evaluate(self, column_name: str, samples: Iterable[str]) -> "RuleEvaluation":
        normalized_name = column_name.lower()
        name_hit = any(pattern.search(normalized_name) for pattern in self.name_patterns)
        total = 0
        matches = 0
        for raw_value in samples:
            if raw_value is None:
                continue
            value = str(raw_value).strip()
            if not value:
                continue
            total += 1
            if self.value_pattern and self.value_pattern.search(value):
                matches += 1
        ratio = (matches / total) if total else 0.0
        score = 0.0
        if name_hit:
            score += self.name_weight
        if self.value_pattern is None and name_hit:
            score = max(score, self.min_confidence)
        elif self.value_pattern is not None and ratio >= self.value_match_ratio:
            score += self.value_weight
        score = min(score, 1.0)
        return RuleEvaluation(
            rule=self,
            name_match=name_hit,
            value_ratio=ratio,
            confidence=score,
            evaluated_values=total,
        )


@dataclass
class RuleEvaluation:
    rule: Rule
    name_match: bool
    value_ratio: float
    confidence: float
    evaluated_values: int

    @property
    def is_match(self) -> bool:
        return self.confidence >= self.rule.min_confidence


def _compile_pattern(pattern: str) -> re.Pattern:
    return re.compile(pattern)


def _load_rule(raw: dict) -> Rule:
    try:
        name = raw["name"]
        tag = raw["tag"]
    except KeyError as exc:
        raise ValueError(f"Rule missing required key: {exc}") from exc

    name_patterns_raw = raw.get("name_patterns") or []
    if not name_patterns_raw:
        raise ValueError(f"Rule '{name}' must define at least one name pattern.")
    name_patterns = [_compile_pattern(pattern) for pattern in name_patterns_raw]

    value_pattern = raw.get("value_pattern")
    compiled_value = _compile_pattern(value_pattern) if value_pattern else None

    min_confidence = float(raw.get("min_confidence", 0.6))
    name_weight = float(raw.get("name_weight", 0.5))
    value_weight = float(raw.get("value_weight", 0.5))
    value_match_ratio = float(raw.get("value_match_ratio", 0.6))
    description = raw.get("description")

    if name_weight + value_weight <= 0:
        raise ValueError(f"Rule '{name}' must have positive weight totals.")
    return Rule(
        name=name,
        tag=tag,
        name_patterns=name_patterns,
        value_pattern=compiled_value,
        min_confidence=min_confidence,
        name_weight=name_weight,
        value_weight=value_weight,
        value_match_ratio=value_match_ratio,
        description=description,
    )


def load_rules(path: str) -> List[Rule]:
    rule_path = Path(path)
    if not rule_path.exists():
        raise FileNotFoundError(f"Rules file not found: {path}")
    content = yaml.safe_load(rule_path.read_text())
    if not content:
        return []
    rules_raw = content.get("rules")
    if rules_raw is None:
        raise ValueError("Rules file must contain a top-level 'rules' list.")
    return [_load_rule(raw_rule) for raw_rule in rules_raw]
