#!/usr/bin/env sh
set -e

# This script runs during cluster init (when PGDATA is empty) via /docker-entrypoint-initdb.d
# It creates least-privilege tenant roles and then executes seed.sql to create schemas/tables/data.

echo "[init.sh] Creating tenant roles..."
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 't001') THEN
    CREATE ROLE t001 LOGIN PASSWORD '${T001_PASSWORD}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 't002') THEN
    CREATE ROLE t002 LOGIN PASSWORD '${T002_PASSWORD}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
END$$;

-- tighten public access
REVOKE ALL ON DATABASE sandbox FROM PUBLIC;
ALTER DATABASE sandbox OWNER TO postgres;
SQL

echo "[init.sh] Running seed.sql..."
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /seed/seed.sql

echo "[init.sh] Done."
