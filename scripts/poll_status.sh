#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  printf '%s\n' "Usage: $0 <dataset_urn> [timeout_seconds]" >&2
  exit 1
fi

DATASET_URN=$1
TIMEOUT=${2:-300}
INTERVAL=${POLL_INTERVAL:-10}
DATAHUB_GMS=${DATAHUB_GMS:-}
DATAHUB_TOKEN=${DATAHUB_TOKEN:-}

if [ -z "$DATAHUB_GMS" ]; then
  printf '%s\n' "DATAHUB_GMS must be set" >&2
  exit 1
fi

payload=$(cat <<JSON
{
  "query": "query dataset($urn: String!) { dataset(urn: $urn) { editableProperties { customProperties } } }",
  "variables": {"urn": "${DATASET_URN}"}
}
JSON
)

curl_args=(
  -sS -X POST "${DATAHUB_GMS%/}/graphql"
  -H "Content-Type: application/json"
  --data "$payload"
)
if [ -n "$DATAHUB_TOKEN" ]; then
  curl_args+=(-H "Authorization: Bearer ${DATAHUB_TOKEN}")
fi

start=$(date +%s)
while true; do
  now=$(date +%s)
  elapsed=$((now - start))
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    printf '%s\n' "Timed out waiting for last_tokenization_run" >&2
    exit 1
  fi
  response=$(curl "${curl_args[@]}")
  raw_status=$(printf '%s' "$response" |
    jq -r '.data.dataset.editableProperties.customProperties.last_tokenization_run // empty' || true)
  if [ -n "$raw_status" ] && [ "$raw_status" != "null" ]; then
    status_value=$(printf '%s' "$raw_status" | jq -r 'fromjson.status' 2>/dev/null || true)
    printf '%s\n' "Current status: ${status_value:-unknown}" >&2
    if [ "$status_value" = "SUCCESS" ]; then
      printf '%s\n' "$raw_status" | jq -c 'fromjson'
      exit 0
    fi
    if [ "$status_value" = "FAILED" ]; then
      printf '%s\n' "$raw_status" | jq -c 'fromjson' >&2
      exit 2
    fi
  else
    printf '%s\n' "Waiting for status..." >&2
  fi
  sleep "$INTERVAL"
done
