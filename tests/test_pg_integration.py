import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import psycopg2
from psycopg2 import extensions
import pytest

from action import db_pg
from action.models import DatasetRef
from action.sdk_adapter import TokenizationSDKAdapter


def _run_as_postgres(args):
    cmd = ["runuser", "-u", "postgres", "--"] + [str(part) for part in args]
    return subprocess.run(cmd, check=True, capture_output=True)


@pytest.fixture(scope="session")
def postgres_server():
    bin_dir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
    initdb = Path(bin_dir) / "initdb"
    pg_ctl = Path(bin_dir) / "pg_ctl"

    tmpdir = Path(tempfile.mkdtemp(prefix="pgdata-"))
    shutil.chown(tmpdir, user="postgres", group="postgres")
    tmpdir.chmod(0o775)
    data_dir = tmpdir

    _run_as_postgres([initdb, "-D", data_dir])

    port = 55432
    logfile = data_dir / "logfile"
    start_cmd = [
        pg_ctl,
        "-D",
        data_dir,
        "-o",
        f"-F -p {port}",
        "-l",
        logfile,
        "start",
    ]
    _run_as_postgres(start_cmd)

    conn_str_admin = f"postgresql://postgres@127.0.0.1:{port}/postgres"

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            with psycopg2.connect(conn_str_admin) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            break
        except psycopg2.OperationalError:
            time.sleep(0.5)
    else:
        raise RuntimeError("Postgres did not start")

    yield {
        "port": port,
        "conn_str_admin": conn_str_admin,
        "pg_ctl": pg_ctl,
        "data_dir": data_dir,
    }

    _run_as_postgres([pg_ctl, "-D", data_dir, "stop", "-m", "fast"])


@pytest.fixture
def seeded_database(postgres_server):
    conn_str_admin = postgres_server["conn_str_admin"]
    conn_str = conn_str_admin
    with psycopg2.connect(conn_str) as conn:
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS schema CASCADE")
            cur.execute("CREATE SCHEMA schema")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS schema.customers ("
                "id SERIAL PRIMARY KEY,"
                "email TEXT,"
                "phone TEXT"
                ")"
            )
            cur.execute("TRUNCATE schema.customers")
            cur.execute(
                "INSERT INTO schema.customers (email, phone) "
                "SELECT 'user' || g::text || '@example.com', '+1-555-' || LPAD(g::text, 4, '0') "
                "FROM generate_series(1, 100) AS g"
            )
    return conn_str


def test_postgres_tokenization_round_trip(seeded_database):
    adapter = TokenizationSDKAdapter(mode="dummy")
    dataset = DatasetRef.from_urn(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.schema.customers,PROD)"
    )

    first = db_pg.tokenize_table(
        seeded_database,
        dataset,
        ["email", "phone"],
        200,
        adapter,
    )
    assert first["updated_count"] == 100
    assert first["skipped_count"] == 0

    second = db_pg.tokenize_table(
        seeded_database,
        dataset,
        ["email", "phone"],
        200,
        adapter,
    )
    assert second["updated_count"] == 0
    assert second["skipped_count"] == 0

    with psycopg2.connect(seeded_database) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT email, phone FROM schema.customers LIMIT 1")
            email, phone = cur.fetchone()
            assert adapter.is_token(email)
            assert adapter.is_token(phone)
