#!/usr/bin/env python3
"""Parse a Databricks JDBC URL into environment assignments."""

from __future__ import annotations

import os
import sys
from urllib.parse import parse_qs

jdbc = os.environ.get("DBX_JDBC_URL")
if not jdbc:
    raise SystemExit("DBX_JDBC_URL is not set")
if not jdbc.startswith("jdbc:databricks://"):
    raise SystemExit("Unsupported JDBC URL")
remainder = jdbc[len("jdbc:databricks://") :]
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
    raise SystemExit("JDBC URL missing httpPath or token")
print(f"INGEST_DBX_SERVER={host}")
print(f"INGEST_DBX_HTTP_PATH={http_path}")
print(f"INGEST_DBX_TOKEN={token}")
