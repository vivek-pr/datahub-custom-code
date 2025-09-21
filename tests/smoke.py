#!/usr/bin/env python3
"""Lightweight smoke test for the DataHub/Postgres encoding PoC.

Run after `make up`. Tries to use psycopg if available, otherwise shells out to
`docker compose exec postgres psql` so no extra Python dependencies are
required.
"""

import base64
import os
import sys
import subprocess
from typing import Iterable, List, Sequence

try:
    import psycopg  # type: ignore
    from psycopg import sql  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    psycopg = None
    sql = None

TEXTUAL_TYPES = {
    "character varying",
    "text",
    "character",
    "varchar",
}


DB_HOST = os.environ.get("SMOKE_DB_HOST", "localhost")
DB_PORT = int(os.environ.get("SMOKE_DB_PORT", "5432"))
DB_NAME = os.environ.get("SMOKE_DB_NAME", "postgres")
DB_USER = os.environ.get("SMOKE_DB_USER", "datahub")
DB_PASSWORD = os.environ.get("SMOKE_DB_PASSWORD", "datahub")


def get_psycopg_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def run_psql(query: str) -> Iterable[List[str]]:
    env = os.environ.copy()
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        DB_USER,
        "-d",
        DB_NAME,
        "-F",
        "|",
        "-A",
        "-t",
        "-c",
        query,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
    for line in proc.stdout.strip().splitlines():
        if line:
            yield line.split("|")


def assert_schema_exists(cursor=None) -> None:
    if cursor is not None:
        cursor.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'encoded'"
        )
        if cursor.fetchone() is None:
            raise AssertionError("encoded schema not found; has the action processed data yet?")
        return

    rows = list(
        run_psql("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'encoded';")
    )
    if not rows:
        raise AssertionError("encoded schema not found; has the action processed data yet?")


def fetch_tables(cursor=None) -> List[str]:
    query = (
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name;"
    )
    if cursor is not None:
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    return [row[0] for row in run_psql(query)]


def fetch_textual_columns(cursor, table: str) -> List[str]:
    if cursor is not None:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND data_type IN %s
            ORDER BY ordinal_position
            """,
            (table, tuple(TEXTUAL_TYPES)),
        )
        return [row[0] for row in cursor.fetchall()]

    sanitized = table.replace("'", "''")
    type_list = ",".join(f"'{t}'" for t in TEXTUAL_TYPES)
    query = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = 'public' AND table_name = '{sanitized}' "
        f"AND data_type IN ({type_list}) ORDER BY ordinal_position;"
    )
    return [row[0] for row in run_psql(query)]


def assert_row_counts(cursor, table: str) -> None:
    if cursor is not None:
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM public.{table}").format(table=sql.Identifier(table))
        )
        source_count = cursor.fetchone()[0]
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM encoded.{table}").format(table=sql.Identifier(table))
        )
        encoded_count = cursor.fetchone()[0]
    else:
        sanitized = table.replace("'", "''")
        source_count = int(
            next(run_psql(f"SELECT COUNT(*) FROM public.{sanitized};"))[0]
        )
        encoded_count = int(
            next(run_psql(f"SELECT COUNT(*) FROM encoded.{sanitized};"))[0]
        )
    print(f"Row counts for {table}: public={source_count}, encoded={encoded_count}")
    if source_count != encoded_count:
        raise AssertionError(
            f"Row count mismatch for {table}: public={source_count}, encoded={encoded_count}"
        )


def assert_contains_base64(cursor, table: str, columns: Sequence[str]) -> None:
    if not columns:
        return
    for column in columns:
        if cursor is not None:
            cursor.execute(
                sql.SQL(
                    "SELECT {column} FROM encoded.{table} WHERE {column} IS NOT NULL LIMIT 10"
                ).format(column=sql.Identifier(column), table=sql.Identifier(table))
            )
            values = [row[0] for row in cursor.fetchall() if isinstance(row[0], str)]
        else:
            sanitized_table = table.replace("'", "''")
            sanitized_column = column.replace('"', '\"')
            query = (
                f'SELECT "{sanitized_column}" FROM encoded.{sanitized_table} '
                f'WHERE "{sanitized_column}" IS NOT NULL LIMIT 10;'
            )
            values = [row[0] for row in run_psql(query) if row and row[0]]
        if not values:
            continue
        if any(is_base64_string(value) for value in values):
            return
    raise AssertionError(f"No Base64 values detected in encoded.{table} textual columns")


def is_base64_string(value: str) -> bool:
    try:
        base64.b64decode(value, validate=True)
        return True
    except Exception:  # pylint: disable=broad-except
        return False


def main() -> int:
    use_psycopg = psycopg is not None
    conn = None
    if use_psycopg:
        try:
            conn = get_psycopg_connection()
        except Exception as exc:  # pylint: disable=broad-except
            print(
                f"psycopg connection failed ({exc}); falling back to docker compose exec",
                file=sys.stderr,
            )
            use_psycopg = False

    if use_psycopg and conn is not None:
        with conn:
            with conn.cursor() as cur:
                assert_schema_exists(cur)
                tables = fetch_tables(cur)
                if not tables:
                    raise AssertionError("No tables discovered in public schema")
                for table in tables:
                    assert_row_counts(cur, table)
                    text_columns = fetch_textual_columns(cur, table)
                    assert_contains_base64(cur, table, text_columns)
                    print(f"✔ {table}: row counts match and Base64 detected")
        return 0

    # Fallback using docker compose + psql.
    assert_schema_exists()
    tables = fetch_tables()
    if not tables:
        raise AssertionError("No tables discovered in public schema")
    for table in tables:
        assert_row_counts(None, table)
        text_columns = fetch_textual_columns(None, table)
        assert_contains_base64(None, table, text_columns)
        print(f"✔ {table}: row counts match and Base64 detected")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as err:
        print(f"Smoke test failed: {err}", file=sys.stderr)
        sys.exit(1)
