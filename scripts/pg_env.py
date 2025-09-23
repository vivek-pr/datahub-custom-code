#!/usr/bin/env python3
"""Parse a PostgreSQL connection string into environment assignments."""

from __future__ import annotations

import os
import sys
from urllib.parse import urlparse

conn = os.environ.get("PG_CONN_STR")
if not conn:
    raise SystemExit("PG_CONN_STR is not set")
parsed = urlparse(conn)
if parsed.scheme not in {"postgresql", "postgres"}:
    raise SystemExit("Unsupported connection string")
user = parsed.username or ""
password = parsed.password or ""
host = parsed.hostname or "localhost"
port = parsed.port or 5432
path = parsed.path.lstrip("/")
print(f"INGEST_PG_USERNAME={user}")
print(f"INGEST_PG_PASSWORD={password}")
print(f"INGEST_PG_HOST={host}")
print(f"INGEST_PG_PORT={port}")
print(f"INGEST_PG_DATABASE={path}")
