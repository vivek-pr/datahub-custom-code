#!/usr/bin/env bash
set -euo pipefail

DATASET_URN=${DATASET_URN:-}
DATASET_NAME=${DATASET_NAME:-public.customers}
DATASET_PLATFORM=${DATASET_PLATFORM:-postgres}
STATUS_TIMEOUT=${STATUS_TIMEOUT:-600}

if [ -z "$DATASET_URN" ]; then
  printf '%s\n' "Resolving dataset URN for ${DATASET_PLATFORM}:${DATASET_NAME}" >&2
  DATASET_URN=$(python3 scripts/find_dataset_urn.py "$DATASET_NAME" --platform "$DATASET_PLATFORM")
fi

if [ -z "$DATASET_URN" ]; then
  printf '%s\n' "Dataset URN could not be determined" >&2
  exit 1
fi

printf '%s\n' "Using dataset URN: ${DATASET_URN}"

scripts/add_tag.sh "$DATASET_URN"
first_status=$(scripts/poll_status.sh "$DATASET_URN" "$STATUS_TIMEOUT")
first_rows=$(printf '%s' "$first_status" | jq -r '.rows_updated')
if [ "$first_rows" -le 0 ]; then
  printf '%s\n' "Expected first run to update rows, saw: $first_status" >&2
  exit 2
fi
printf '%s\n' "First run updated ${first_rows} rows"

scripts/add_tag.sh "$DATASET_URN"
second_status=$(scripts/poll_status.sh "$DATASET_URN" "$STATUS_TIMEOUT")
second_rows=$(printf '%s' "$second_status" | jq -r '.rows_updated')
if [ "$second_rows" -ne 0 ]; then
  printf '%s\n' "Expected idempotent run to update zero rows, saw: $second_status" >&2
  exit 3
fi
printf '%s\n' "Second run confirmed idempotency"
