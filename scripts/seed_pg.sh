#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${1:-tokenize-poc}
POD=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')

SQL=$(cat <<'SQL'
DO $$
DECLARE
  existing INTEGER;
BEGIN
  SELECT COUNT(*) INTO existing FROM schema.customers;
  IF existing = 0 THEN
    INSERT INTO schema.customers (email, phone)
    SELECT
      'user' || g::text || '@example.com',
      '+1-555-' || LPAD(g::text, 4, '0')
    FROM generate_series(1, 100) AS g;
  END IF;
END $$;
SQL
)

kubectl -n "$NAMESPACE" exec "$POD" -- bash -c "psql postgresql://tokenize:tokenize@localhost:5432/tokenize -c \"$SQL\""
