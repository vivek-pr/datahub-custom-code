"""Command-line interface for the PII classifier."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from typing import List

from .classifier import PIIClassifier
from .config import ClassifierConfig

LOGGER = logging.getLogger(__name__)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Regex-based PII classifier for DataHub schema fields.")
    parser.add_argument("--rules", dest="rules_path", help="Path to the YAML rules file.")
    parser.add_argument("--schemas", help="Comma-separated schema list to scan (overrides env).")
    parser.add_argument("--sample-limit", type=int, dest="sample_limit", help="Max rows sampled per column.")
    parser.add_argument("--min-samples", type=int, dest="min_samples", help="Minimum non-empty values required per column.")
    parser.add_argument("--platform", help="Override DataHub platform (default postgres).")
    parser.add_argument("--env", dest="env", help="Override DataHub env (default PROD).")
    parser.add_argument("--dry-run", action="store_true", help="Do not emit tags; log proposed actions only.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, ...).")
    return parser.parse_args(argv)


def _apply_overrides(config: ClassifierConfig, args: argparse.Namespace) -> ClassifierConfig:
    if args.rules_path:
        config = replace(config, rules_path=args.rules_path)
    if args.schemas:
        schemas = [schema.strip() for schema in args.schemas.split(",") if schema.strip()]
        config.postgres.schemas = schemas
    if args.sample_limit:
        config.postgres.sample_limit = args.sample_limit
    if args.min_samples:
        config.min_value_samples = args.min_samples
    if args.platform:
        config.datahub.platform = args.platform
    if args.env:
        config.datahub.env = args.env
    if args.dry_run:
        config.datahub.dry_run = True
    return config


def main(argv: List[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    _configure_logging(args.log_level)
    config = ClassifierConfig.from_env(args.rules_path)
    config = _apply_overrides(config, args)
    LOGGER.info(
        "Starting PII classifier: schemas=%s sample_limit=%s min_samples=%s dry_run=%s",
        config.postgres.schemas,
        config.postgres.sample_limit,
        config.min_value_samples,
        config.datahub.dry_run,
    )
    classifier = PIIClassifier(config)
    matches = classifier.run()
    for match in matches:
        LOGGER.info(
            "Tagged %s.%s.%s field=%s with %s (confidence=%.2f)",
            config.postgres.database,
            match.column.schema,
            match.column.table,
            match.column.name,
            match.evaluation.rule.tag,
            match.evaluation.confidence,
        )
    LOGGER.info("Finished. total_matches=%s", len(matches))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
