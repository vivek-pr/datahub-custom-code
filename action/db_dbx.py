"""Databricks SQL Warehouse integration."""

from __future__ import annotations

import logging
from typing import Dict, List, Sequence
from urllib.parse import parse_qs

from databricks import sql as dbsql

from .models import DatasetRef, TokenizationResult
from .sdk_adapter import TokenizationSDKAdapter
from .token_logic import TOKEN_REGEX

LOGGER = logging.getLogger(__name__)


def parse_jdbc_url(url: str) -> Dict[str, str]:
    if not url.startswith("jdbc:databricks://"):
        raise ValueError("Unsupported Databricks JDBC URL")
    remainder = url[len("jdbc:databricks://") :]
    if "?" in remainder:
        host_part, query = remainder.split("?", 1)
    elif ";" in remainder:
        host_part, query = remainder.split(";", 1)
    else:
        host_part, query = remainder, ""
    host = host_part.split("/")[0]
    if ":" in host:
        host = host.split(":", 1)[0]
    query = query.replace(";", "&")
    params = parse_qs(query, keep_blank_values=True)
    http_path = params.get("httpPath", [None])[0]
    token = params.get("PWD", [None])[0] or params.get("pwd", [None])[0]
    if not http_path or not token:
        raise ValueError("JDBC URL must contain httpPath and PWD parameters")
    return {"host": host, "http_path": http_path, "token": token}


def tokenize_table(
    jdbc_url: str,
    dataset: DatasetRef,
    columns: Sequence[str],
    limit: int,
    adapter: TokenizationSDKAdapter,
) -> TokenizationResult:
    parsed = parse_jdbc_url(jdbc_url)
    updated_rows = 0
    skipped_rows = 0

    LOGGER.info("Connecting to Databricks warehouse at %s", parsed["host"])
    with dbsql.connect(
        server_hostname=parsed["host"],
        http_path=parsed["http_path"],
        access_token=parsed["token"],
    ) as connection:
        with connection.cursor() as cursor:
            where_clauses = []
            params: List[str] = []
            for column in columns:
                where_clauses.append(f"({column} IS NOT NULL AND {column} NOT RLIKE ?)")
                params.append(TOKEN_REGEX.pattern)
            select_sql = (
                f"SELECT id, {', '.join(columns)} FROM {dataset.table_expression} "
                f"WHERE {' OR '.join(where_clauses)} LIMIT ?"
            )
            params.append(limit)
            cursor.execute(select_sql, params)
            rows = cursor.fetchall()
            LOGGER.info("Fetched %s candidate rows from Databricks", len(rows))

            for row in rows:
                row_id = row[0]
                current_values = dict(zip(columns, row[1:]))
                updates: Dict[str, str] = {}
                for column, value in current_values.items():
                    if value is None:
                        continue
                    if adapter.is_token(value):
                        continue
                    updates[column] = adapter.tokenize(value)
                if not updates:
                    skipped_rows += 1
                    continue
                assignments = ", ".join(f"{column} = ?" for column in updates)
                update_sql = (
                    f"UPDATE {dataset.table_expression} SET {assignments} WHERE id = ?"
                )
                cursor.execute(update_sql, list(updates.values()) + [row_id])
                updated_rows += 1
        connection.commit()

    LOGGER.info(
        "Databricks tokenization complete: %s updated, %s skipped",
        updated_rows,
        skipped_rows,
    )
    return TokenizationResult(
        columns=columns, rows_updated=updated_rows, rows_skipped=skipped_rows
    )
