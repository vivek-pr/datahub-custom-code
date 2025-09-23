#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  printf '%s\n' "Usage: $0 <dataset_urn>" >&2
  exit 1
fi

DATASET_URN=$1
DATAHUB_GMS=${DATAHUB_GMS:-}
DATAHUB_TOKEN=${DATAHUB_TOKEN:-}
TAG_URN="urn:li:tag:tokenize/run"

if [ -z "$DATAHUB_GMS" ]; then
  printf '%s\n' "DATAHUB_GMS must be set" >&2
  exit 1
fi

payload=$(cat <<JSON
{
  "query": "mutation addTag($input: TagAssociationInput!) { addTag(input: $input) { __typename } }",
  "variables": {
    "input": {
      "tagUrn": "${TAG_URN}",
      "resourceUrn": "${DATASET_URN}"
    }
  }
}
JSON
)

printf '%s\n' "Adding tokenize/run tag to ${DATASET_URN}"

curl_args=(
  -sS -X POST "${DATAHUB_GMS%/}/graphql"
  -H "Content-Type: application/json"
  --data "$payload"
)
if [ -n "$DATAHUB_TOKEN" ]; then
  curl_args+=(-H "Authorization: Bearer ${DATAHUB_TOKEN}")
fi

curl "${curl_args[@]}" >/tmp/add_tag_response.json

if jq -e '.errors' >/dev/null 2>&1 < /tmp/add_tag_response.json; then
  printf '%s\n' "Failed to add tag:" >&2
  cat /tmp/add_tag_response.json >&2
  exit 1
fi

printf '%s\n' "Tag added successfully"
