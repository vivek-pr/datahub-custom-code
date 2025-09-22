"""End-to-end test covering ingestion, classification, and encoding."""

from __future__ import annotations

import base64
import subprocess
import time
from pathlib import Path
from typing import Sequence

import psycopg
from psycopg import sql

from actions.base64_action.configuration import RuntimeOverrides
from scripts.run_classifier_and_encode import run_once

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "datahub"
POSTGRES_PASSWORD = "datahub"
PIPELINE_NAME = "postgres_local_poc"
SCHEMA = "public"
TABLE = "customers"
REPO_ROOT = Path(__file__).resolve().parents[1]


def _connect():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        autocommit=True,
    )


def _prepare_source_table() -> None:
    rows: Sequence[tuple[str, str, str, str]] = [
        ("Ada Lovelace", "ada@example.com", "+1-555-123-0001", "ID-A12345"),
        ("Alan Turing", "alan@bombe.test", "+1-555-123-0002", "ID-B22345"),
        ("Grace Hopper", "grace@navy.mil", "+1-555-123-0003", "ID-C32345"),
        ("Dorothy Vaughan", "dorothy@nasa.gov", "+1-555-123-0004", "ID-D42345"),
        ("Katherine Johnson", "katherine@nasa.gov", "+1-555-123-0005", "ID-E52345"),
    ]
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE").format(
                    schema=sql.Identifier(SCHEMA),
                    table=sql.Identifier(TABLE),
                )
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE {schema}.{table} (
                        id SERIAL PRIMARY KEY,
                        full_name TEXT,
                        email TEXT,
                        phone_number VARCHAR(32),
                        notes TEXT,
                        reference_code TEXT
                    )
                    """
                ).format(
                    schema=sql.Identifier(SCHEMA),
                    table=sql.Identifier(TABLE),
                )
            )
            cur.executemany(
                sql.SQL(
                    "INSERT INTO {schema}.{table} (full_name, email, phone_number, notes, reference_code)"
                    " VALUES (%s, %s, %s, %s, %s)"
                ).format(
                    schema=sql.Identifier(SCHEMA),
                    table=sql.Identifier(TABLE),
                ),
                [(name, email, phone, f"Notes for {name}", ref) for name, email, phone, ref in rows],
            )


def _run_ingestion() -> None:
    subprocess.run(
        ["docker", "compose", "run", "--rm", "ingestion"],
        check=True,
        cwd=REPO_ROOT,
    )


def _fetch_column_values(schema: str, table: str, columns: Sequence[str]) -> list[tuple]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT {cols} FROM {schema}.{table} ORDER BY id"
                ).format(
                    cols=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                )
            )
            return cur.fetchall()


def _decode(value: str) -> str:
    return base64.b64decode(value).decode("utf-8")


def test_ingestion_classifier_and_encoder_flow():
    _prepare_source_table()
    _run_ingestion()
    time.sleep(5)

    overrides = RuntimeOverrides(
        pipeline_name=PIPELINE_NAME,
        platform="postgres",
        database_host=POSTGRES_HOST,
        database_port=POSTGRES_PORT,
        database_name=POSTGRES_DB,
        database_user=POSTGRES_USER,
        database_password=POSTGRES_PASSWORD,
        schema_allowlist=["public.*"],
    )
    allowlist = run_once(
        pipeline_name=PIPELINE_NAME,
        platform="postgres",
        schema_allowlist=["public.*"],
        overrides=overrides,
    )

    key = (SCHEMA, TABLE)
    assert key in allowlist
    flagged = {column.lower() for column in allowlist[key]}
    assert "email" in flagged
    assert "phone_number" in flagged
    assert "reference_code" in flagged
    assert "notes" not in flagged

    encoded_rows = _fetch_column_values("encoded", TABLE, ["email", "phone_number", "notes", "reference_code"])
    source_rows = _fetch_column_values(SCHEMA, TABLE, ["email", "phone_number", "notes", "reference_code"])
    assert len(encoded_rows) == len(source_rows)

    for encoded, source in zip(encoded_rows, source_rows, strict=True):
        encoded_email, encoded_phone, encoded_notes, encoded_ref = encoded
        src_email, src_phone, src_notes, src_ref = source
        assert _decode(encoded_email) == src_email
        assert _decode(encoded_phone) == src_phone
        assert encoded_notes == src_notes
        assert _decode(encoded_ref) == src_ref
