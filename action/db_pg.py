"""Postgres integration for the tokenization action."""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Tuple

import psycopg2
from psycopg2 import sql

from .models import DatasetRef
from .sdk_adapter import TokenizationSDKAdapter
from .token_logic import TOKEN_REGEX

logger = logging.getLogger(__name__)


def _build_select_query(
    dataset: DatasetRef, columns: Iterable[str], limit: int
) -> Tuple[sql.SQL, List[str]]:
    table = sql.SQL("{}.{}").format(
        sql.Identifier(dataset.schema), sql.Identifier(dataset.table)
    )
    select_cols = [sql.Identifier("id")] + [
        sql.Identifier(column) for column in columns
    ]
    regex_params: List[str] = []
    conditions = []
    for column in columns:
        conditions.append(
            sql.SQL("({col} IS NOT NULL AND {col} !~ %s)").format(
                col=sql.Identifier(column)
            )
        )
        regex_params.append(TOKEN_REGEX.pattern)
    query = sql.SQL(
        "SELECT {select_cols} FROM {table} WHERE {condition} ORDER BY id LIMIT %s FOR UPDATE"
    ).format(
        select_cols=sql.SQL(", ").join(select_cols),
        table=table,
        condition=sql.SQL(" OR ").join(conditions),
    )
    params = regex_params + [limit]
    return query, params


def tokenize_table(
    conn_str: str,
    dataset: DatasetRef,
    columns: List[str],
    limit: int,
    adapter: TokenizationSDKAdapter,
) -> Dict[str, int]:
    updated_rows = 0
    skipped_rows = 0

    logger.info("Connecting to Postgres for dataset %s", dataset.table_expression)
    with psycopg2.connect(conn_str) as connection:
        connection.autocommit = False
        with connection.cursor() as cursor:
            select_query, params = _build_select_query(dataset, columns, limit)
            cursor.execute(select_query, params)
            rows = cursor.fetchall()
            logger.info("Fetched %s candidate rows from Postgres", len(rows))

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

                assignments = [
                    sql.SQL("{} = %s").format(sql.Identifier(column))
                    for column in updates
                ]
                update_query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(
                    sql.SQL("{}.{}").format(
                        sql.Identifier(dataset.schema), sql.Identifier(dataset.table)
                    ),
                    sql.SQL(", ").join(assignments),
                )
                cursor.execute(update_query, list(updates.values()) + [row_id])
                updated_rows += 1

        connection.commit()

    logger.info(
        "Postgres tokenization complete: %s updated, %s skipped",
        updated_rows,
        skipped_rows,
    )
    return {"updated_count": updated_rows, "skipped_count": skipped_rows}
