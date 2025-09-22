#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./scripts/trigger.sh --dataset <dataset-urn> --columns col1,col2 [--limit 100] [--namespace tokenize-poc]
USAGE
}

DATASET=""
COLUMNS_RAW=""
LIMIT=100
NAMESPACE=tokenize-poc

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset)
      DATASET="$2"
      shift 2
      ;;
    --columns)
      COLUMNS_RAW="$2"
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$DATASET" || -z "$COLUMNS_RAW" ]]; then
  usage
  exit 1
fi

export DATASET COLUMNS_RAW LIMIT
PAYLOAD=$(python - <<'PY'
import json, os
cols = [c.strip() for c in os.environ["COLUMNS_RAW"].split(",") if c.strip()]
if not cols:
    raise SystemExit("column list may not be empty")
payload = {
    "dataset": os.environ["DATASET"],
    "columns": cols,
    "limit": int(os.environ.get("LIMIT", "100")),
}
print(json.dumps(payload))
PY
)

kubectl run --namespace "$NAMESPACE" trigger-once --rm -i --restart=Never --image=curlimages/curl:8.7.1 --command -- sh -c "curl -sS -X POST -H 'Content-Type: application/json' -d '$PAYLOAD' http://tokenize-poc-action:8080/trigger" | python -m json.tool
