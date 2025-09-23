#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${1:-tokenize-poc}
POD=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')

echo "Seeding Postgres data in namespace $NAMESPACE"

kubectl -n "$NAMESPACE" exec "$POD" -- bash -c "cat <<'SQL' | psql postgresql://tokenize:tokenize@localhost:5432/tokenize -v ON_ERROR_STOP=1
CREATE SCHEMA IF NOT EXISTS schema;
CREATE TABLE IF NOT EXISTS schema.customers (
  id SERIAL PRIMARY KEY,
  email TEXT,
  phone TEXT
);
WITH inserted AS (
  INSERT INTO schema.customers (email, phone)
  SELECT
    'user' || g::text || '@example.com',
    '+1-555-' || LPAD(g::text, 4, '0')
  FROM generate_series(1, 100) AS g
  ON CONFLICT DO NOTHING
  RETURNING 1
)
SELECT COUNT(*) AS inserted_rows FROM inserted;
SQL"
