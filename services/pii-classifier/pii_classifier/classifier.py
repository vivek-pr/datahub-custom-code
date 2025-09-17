"""Core classification workflow."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from .config import ClassifierConfig
from .emitter import DataHubTagEmitter, TagUpsertResult
from .postgres_sampler import ColumnMetadata, PostgresSampler
from .rules_loader import RuleEvaluation, load_rules

LOGGER = logging.getLogger(__name__)


@dataclass
class FieldMatch:
    dataset_name: str
    column: ColumnMetadata
    evaluation: RuleEvaluation
    emission: TagUpsertResult


class PIIClassifier:
    def __init__(self, config: ClassifierConfig):
        self._config = config
        self._rules = load_rules(config.rules_path)
        self._emitter = DataHubTagEmitter(config.datahub)

    def run(self) -> List[FieldMatch]:
        if not self._rules:
            LOGGER.warning("No rules loaded; classifier will exit without tagging.")
            return []
        tag_specs = [
            (rule.tag, rule.name.replace("_", " ").title(), rule.description) for rule in self._rules
        ]
        self._emitter.ensure_tag_definitions(tag_specs)
        matches: List[FieldMatch] = []
        with PostgresSampler(self._config.postgres) as sampler:
            for column in sampler.iter_columns():
                dataset_name = self._dataset_name_for(column)
                samples = sampler.sample_values(column, self._config.postgres.sample_limit)
                if not samples:
                    continue
                evaluation = self._evaluate(column.name, samples)
                if evaluation is None or not evaluation.is_match:
                    continue
                reason = self._format_reason(evaluation)
                emission = self._emitter.add_field_tag(
                    dataset_name=dataset_name,
                    field=column.name,
                    tag_urn=evaluation.rule.tag,
                    confidence=evaluation.confidence,
                    rule_name=evaluation.rule.name,
                    reason=reason,
                )
                matches.append(FieldMatch(dataset_name, column, evaluation, emission))
        LOGGER.info("Completed classification. %s matches.", len(matches))
        return matches

    def _dataset_name_for(self, column: ColumnMetadata) -> str:
        return f"{self._config.postgres.database}.{column.schema}.{column.table}"

    def _evaluate(self, column_name: str, samples: List[str]) -> RuleEvaluation | None:
        filtered_samples = [value for value in samples if value is not None and str(value).strip()]
        if len(filtered_samples) < self._config.min_value_samples:
            LOGGER.debug(
                "Skipping column %s: only %s non-empty samples (min=%s)",
                column_name,
                len(filtered_samples),
                self._config.min_value_samples,
            )
            return None
        best: RuleEvaluation | None = None
        for rule in self._rules:
            evaluation = rule.evaluate(column_name, filtered_samples)
            LOGGER.debug(
                "Evaluated rule=%s column=%s confidence=%.2f ratio=%.2f name_match=%s",
                rule.name,
                column_name,
                evaluation.confidence,
                evaluation.value_ratio,
                evaluation.name_match,
            )
            if not evaluation.is_match:
                continue
            if best is None or evaluation.confidence > best.confidence:
                best = evaluation
        return best

    @staticmethod
    def _format_reason(evaluation: RuleEvaluation) -> str:
        return (
            f"name_match={evaluation.name_match} "
            f"value_ratio={evaluation.value_ratio:.2f} "
            f"evaluated={evaluation.evaluated_values}"
        )
