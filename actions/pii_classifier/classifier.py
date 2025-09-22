"""Simple regex-based PII classifier for Postgres tables."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import psycopg
from psycopg import Cursor, sql
import yaml

from actions.base64_action.configuration import DatabaseConfig

LOGGER = logging.getLogger("pii-classifier")

DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yml")
DEFAULT_PATTERNS_PATH = Path("classifier/patterns.yml")
TEXTUAL_TYPES = {"text", "character varying", "character", "varchar"}


@dataclass
class PatternRule:
    """Represents a single regex rule from the patterns file."""

    name: str
    column_regex: Optional[re.Pattern]
    value_regex: Optional[re.Pattern]


@dataclass
class ClassifierConfig:
    """Runtime knobs for the regex classifier."""

    patterns_path: Path = DEFAULT_PATTERNS_PATH
    platform: str = "postgres"
    pipeline_name_filter: Optional[str] = None
    sample_rows: int = 200
    min_matches: int = 5

    @classmethod
    def load(cls, path: Path = DEFAULT_CONFIG_PATH) -> "ClassifierConfig":
        raw: Dict = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                raw_data = yaml.safe_load(handle) or {}
                if isinstance(raw_data, dict):
                    raw = raw_data
        patterns_path = Path(raw.get("patterns_path", DEFAULT_PATTERNS_PATH))
        sample_rows = int(raw.get("sample_rows", cls.sample_rows))
        min_matches = int(raw.get("min_matches", cls.min_matches))
        pipeline_name_filter = raw.get("pipeline_name_filter")
        platform = raw.get("platform", cls.platform)

        sample_rows = int(os.environ.get("PII_SAMPLE_ROWS", sample_rows))
        min_matches = int(os.environ.get("PII_MIN_MATCHES", min_matches))
        pipeline_name_filter = os.environ.get(
            "PIPELINE_NAME_FILTER", pipeline_name_filter
        )

        return cls(
            patterns_path=patterns_path,
            platform=platform,
            pipeline_name_filter=pipeline_name_filter,
            sample_rows=sample_rows,
            min_matches=min_matches,
        )


class RegexPIIClassifier:
    """Classifies columns as PII using configurable regex patterns."""

    def __init__(self, config: ClassifierConfig, database: DatabaseConfig) -> None:
        self.config = config
        self.database = database
        self.rules: List[PatternRule] = self._load_rules(config.patterns_path)

    def classify(
        self, tables: Iterable[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Set[str]]:
        results: Dict[Tuple[str, str], Set[str]] = {}
        tables = list(tables)
        if not tables:
            LOGGER.info("Classifier received no tables; skipping")
            return results
        LOGGER.info(
            "Scanning %d table(s) for PII using %d regex rules", len(tables), len(self.rules)
        )
        with psycopg.connect(
            host=self.database.host,
            port=self.database.port,
            dbname=self.database.dbname,
            user=self.database.user,
            password=self.database.password,
        ) as conn:
            with conn.cursor() as cur:
                for schema, table in tables:
                    flagged = self._classify_table(cur, schema, table)
                    if flagged:
                        results[(schema, table)] = flagged
                        LOGGER.info(
                            "Table %s.%s flagged columns: %s",
                            schema,
                            table,
                            ", ".join(sorted(flagged)),
                        )
                    else:
                        LOGGER.info("Table %s.%s had no PII matches", schema, table)
        return results

    def _classify_table(
        self, cursor: Cursor, schema: str, table: str
    ) -> Set[str]:
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        columns = cursor.fetchall()
        if not columns:
            LOGGER.debug("No columns found for %s.%s", schema, table)
            return set()
        flagged: Set[str] = set()
        to_sample: Dict[str, List[PatternRule]] = {}
        for column_name, data_type in columns:
            lower_name = column_name.lower()
            matched = False
            for rule in self.rules:
                if rule.column_regex and rule.column_regex.search(lower_name):
                    flagged.add(column_name)
                    matched = True
                    LOGGER.debug(
                        "Column %s.%s.%s matched name rule '%s'", schema, table, column_name, rule.name
                    )
                    break
            if matched:
                continue
            if not self._is_textual_type(data_type):
                continue
            # Collect value-based rules for this column.
            value_rules = [rule for rule in self.rules if rule.value_regex]
            if value_rules:
                to_sample[column_name] = value_rules

        if not to_sample:
            return flagged

        sample_columns = list(to_sample)
        sample_query = sql.SQL("SELECT {cols} FROM {schema}.{table} LIMIT %s").format(
            cols=sql.SQL(", ").join(sql.Identifier(col) for col in sample_columns),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
        cursor.execute(sample_query, (self.config.sample_rows,))
        rows = cursor.fetchall() or []
        if not rows:
            LOGGER.debug("No sample data returned for %s.%s", schema, table)
            return flagged

        for index, column in enumerate(sample_columns):
            values = [row[index] for row in rows if row[index] is not None]
            if not values:
                continue
            for rule in to_sample[column]:
                pattern = rule.value_regex
                if not pattern:
                    continue
                matches = sum(
                    1
                    for value in values
                    if isinstance(value, str) and pattern.search(value)
                )
                if matches >= self.config.min_matches:
                    flagged.add(column)
                    LOGGER.debug(
                        "Column %s.%s.%s matched value rule '%s' (%d/%d)",
                        schema,
                        table,
                        column,
                        rule.name,
                        matches,
                        len(values),
                    )
                    break
        return flagged

    def _load_rules(self, path: Path) -> List[PatternRule]:
        if path.is_absolute():
            candidates = [path]
        else:
            repo_root = Path(__file__).resolve().parents[2]
            candidates = [Path.cwd() / path, repo_root / path]
        resolved = next((candidate for candidate in candidates if candidate.exists()), None)
        if resolved is None:
            raise FileNotFoundError(f"Pattern file not found: {path}")
        with resolved.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        if not isinstance(raw, dict):
            raise ValueError("Pattern file must contain a mapping of rules")
        rules: List[PatternRule] = []
        for name, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            column_pattern = payload.get("column")
            value_pattern = payload.get("value")
            column_regex = re.compile(column_pattern) if column_pattern else None
            value_regex = re.compile(value_pattern) if value_pattern else None
            rules.append(PatternRule(name=name, column_regex=column_regex, value_regex=value_regex))
        return rules

    @staticmethod
    def _is_textual_type(data_type: str) -> bool:
        normalized = (data_type or "").lower()
        if normalized in TEXTUAL_TYPES:
            return True
        return any(keyword in normalized for keyword in ("char", "text"))


__all__ = ["ClassifierConfig", "RegexPIIClassifier", "PatternRule", "TEXTUAL_TYPES"]
