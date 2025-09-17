"""Read schema metadata and sample values from Postgres."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, List, Sequence

import psycopg2
from psycopg2 import sql

from .config import PostgresConfig


@dataclass
class ColumnMetadata:
    schema: str
    table: str
    name: str
    data_type: str


class PostgresSampler:
    def __init__(self, config: PostgresConfig):
        self._config = config
        self._conn = None

    def __enter__(self) -> "PostgresSampler":
        self._conn = psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.database,
            user=self._config.user,
            password=self._config.password,
        )
        self._conn.autocommit = True
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def connection(self):
        if self._conn is None:
            raise RuntimeError("Sampler must be used as a context manager.")
        return self._conn

    def list_tables(self) -> Sequence[tuple[str, str]]:
        query = sql.SQL(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        )
        with self.connection.cursor() as cur:
            cur.execute(query, (self._config.schemas,))
            return cur.fetchall()

    def list_columns(self, schema: str, table: str) -> List[ColumnMetadata]:
        query = sql.SQL(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
        )
        with self.connection.cursor() as cur:
            cur.execute(query, (schema, table))
            rows = cur.fetchall()
        return [ColumnMetadata(schema=schema, table=table, name=row[0], data_type=row[1]) for row in rows]

    def sample_values(self, column: ColumnMetadata, limit: int) -> List[str]:
        table_identifier = sql.Identifier(column.schema, column.table)
        column_identifier = sql.Identifier(column.name)
        query = sql.SQL("SELECT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT {limit}").format(
            column=column_identifier,
            table=table_identifier,
            limit=sql.Literal(limit),
        )
        with self.connection.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

    def iter_columns(self) -> Iterator[ColumnMetadata]:
        for schema, table in self.list_tables():
            for column in self.list_columns(schema, table):
                yield column
