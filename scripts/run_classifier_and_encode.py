"""Execute the regex-based PII classifier followed by Base64 encoding."""

from __future__ import annotations

import argparse
import logging
from dataclasses import asdict
from typing import Dict, Iterable, Optional, Sequence, Set, Tuple

from actions.base64_action.action import Base64EncodeAction
from actions.base64_action.configuration import ActionConfig, RuntimeOverrides
from actions.pii_classifier import ClassifierConfig, RegexPIIClassifier

LOGGER = logging.getLogger("pii-flow")


def _clone_overrides(base: Optional[RuntimeOverrides]) -> RuntimeOverrides:
    data = asdict(base) if base else {}
    return RuntimeOverrides(**data)


def _normalize_schema_allowlist(values: Optional[Sequence[str]]) -> Optional[Sequence[str]]:
    if values is None:
        return None
    normalized = [value for value in (value.strip() for value in values if value) if value]
    return normalized or None


def run_once(
    pipeline_name: Optional[str] = None,
    platform: Optional[str] = None,
    schema_allowlist: Optional[Sequence[str]] = None,
    overrides: Optional[RuntimeOverrides] = None,
) -> Dict[Tuple[str, str], Set[str]]:
    """Run the classifier and encoder once. Returns the encoded columns."""

    classifier_config = ClassifierConfig.load()
    candidate_pipeline = pipeline_name or (overrides.pipeline_name if overrides else None)
    filter_name = classifier_config.pipeline_name_filter
    if filter_name and candidate_pipeline and filter_name.lower() != candidate_pipeline.lower():
        LOGGER.info(
            "Pipeline %s ignored because it does not match configured filter %s",
            candidate_pipeline,
            filter_name,
        )
        return {}

    effective = _clone_overrides(overrides)
    if pipeline_name is not None:
        effective.pipeline_name = pipeline_name
    if platform is not None:
        effective.platform = platform
    if schema_allowlist is not None:
        effective.schema_allowlist = _normalize_schema_allowlist(schema_allowlist)

    if not effective.platform:
        effective.platform = classifier_config.platform
    if not effective.pipeline_name:
        effective.pipeline_name = classifier_config.pipeline_name_filter

    action_config = ActionConfig.load(overrides=effective)
    classifier = RegexPIIClassifier(classifier_config, action_config.database)
    action = Base64EncodeAction(action_config)

    try:
        identifiers = action.list_matching_datasets()
        if not identifiers:
            LOGGER.info("No datasets discovered for platform %s", action_config.platform)
            return {}
        table_keys = [(identifier.schema, identifier.table) for identifier in identifiers]
        flagged = classifier.classify(table_keys)
        if not flagged:
            LOGGER.info(
                "Classifier completed with zero matches across %d table(s)",
                len(table_keys),
            )
            return {}
        normalized: Dict[Tuple[str, str], Set[str]] = {}
        for (schema, table), columns in flagged.items():
            key = (schema.lower(), table.lower())
            normalized[key] = set(columns)
        LOGGER.info(
            "Classifier marked %d column(s) across %d table(s)",
            sum(len(columns) for columns in normalized.values()),
            len(normalized),
        )
        allowlist: Dict[Tuple[str, str], Set[str]] = {}
        for identifier in identifiers:
            key = (identifier.schema.lower(), identifier.table.lower())
            columns = normalized.get(key)
            if not columns:
                continue
            allowlist[(identifier.schema, identifier.table)] = columns
        if not allowlist:
            LOGGER.info(
                "Classifier results did not match any ingested tables after normalization"
            )
            return {}
        LOGGER.info(
            "Encoding %d table(s) with explicit column allowlist", len(allowlist)
        )
        action.process_with_allowlist(allowlist)
        return allowlist
    finally:
        action.conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the regex-based PII classifier and Base64 encoder",
    )
    parser.add_argument("--pipeline-name", help="Pipeline name from the ingestion event")
    parser.add_argument("--platform", help="DataHub platform (default from config)")
    parser.add_argument(
        "--schema-allow",
        action="append",
        dest="schema_allow",
        help="Schema allowlist patterns (can be specified multiple times)",
    )
    return parser


def main(args: Optional[Iterable[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parsed = _build_parser().parse_args(args=args)
    overrides = RuntimeOverrides()
    allowlist = run_once(
        pipeline_name=parsed.pipeline_name,
        platform=parsed.platform,
        schema_allowlist=parsed.schema_allow,
        overrides=overrides,
    )
    if allowlist:
        LOGGER.info(
            "Encoding complete. Tables processed: %s",
            ", ".join(f"{schema}.{table}" for schema, table in allowlist),
        )
    else:
        LOGGER.info("Encoding skipped; no PII columns detected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
