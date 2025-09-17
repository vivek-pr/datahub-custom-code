#!/usr/bin/env bash
set -euo pipefail

# Simple health check script for DataHub on Kubernetes/Minikube.
# - Probes GraphiQL (/api/graphiql) and GraphQL (/api/graphql) on GMS
# - Optionally pings a REST endpoint as a liveness check
#
# Usage: scripts/check_datahub.sh [--namespace <ns>] [--gms-url <url>] [--timeout <sec>]

NS="datahub"
GMS_URL=""
TIMEOUT=60
RELEASE=""
SERVICE_NAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace|-n)
      NS="$2"; shift 2 ;;
    --gms-url)
      GMS_URL="$2"; shift 2 ;;
    --timeout)
      TIMEOUT="$2"; shift 2 ;;
    --release|-r)
      RELEASE="$2"; shift 2 ;;
    --service)
      SERVICE_NAME="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required tool: $1" >&2
    exit 1
  fi
}

require_tool kubectl
require_tool minikube
require_tool curl

# Derive GMS URL if not provided explicitly
if [[ -z "$GMS_URL" ]]; then
  if [[ -n "$SERVICE_NAME" ]]; then
    SVC_NAME="$SERVICE_NAME"
  elif [[ -n "$RELEASE" ]]; then
    SVC_NAME="${RELEASE}-datahub-gms"
  else
    SVC_NAME=$(kubectl -n "$NS" get svc -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | grep -E -- '-datahub-gms$' | head -n1 || true)
    if [[ -z "$SVC_NAME" ]]; then
      SVC_NAME="datahub-gms"
    fi
  fi

  if ! kubectl -n "$NS" get svc "$SVC_NAME" >/dev/null 2>&1; then
    echo "Could not find GMS service '$SVC_NAME' in namespace '$NS'." >&2
    echo "Available services:" >&2
    kubectl -n "$NS" get svc >&2 || true
    exit 1
  fi

  SVC_TYPE=$(kubectl -n "$NS" get svc "$SVC_NAME" -o jsonpath='{.spec.type}' 2>/dev/null || echo "")
  if [[ "$SVC_TYPE" == "ClusterIP" ]]; then
    echo "Service '$SVC_NAME' is of type ClusterIP and not directly accessible. Provide --gms-url or expose the service via NodePort/LoadBalancer." >&2
    exit 1
  fi

  set +e
  URLS=$(minikube service -n "$NS" "$SVC_NAME" --url 2>/dev/null)
  RC=$?
  set -e
  if [[ $RC -ne 0 || -z "$URLS" ]]; then
    echo "Could not derive GMS URL via minikube service. Ensure the service is exposed: $SVC_NAME in ns $NS" >&2
    exit 1
  fi
  GMS_URL=$(echo "$URLS" | head -n1)
fi

echo "Using GMS URL: $GMS_URL"

deadline=$(( $(date +%s) + TIMEOUT ))

http_ok() {
  local url="$1"
  curl -fsS -o /dev/null -w "%{http_code}" "$url" || true
}

graphql_post() {
  local url="$1"
  local body='{"query":"query { __typename }"}'
  curl -fsS -o /dev/null -w "%{http_code}" -H 'Content-Type: application/json' -X POST --data "$body" "$url" || true
}

echo "Checking GraphiQL (/api/graphiql) availability..."
while true; do
  code=$(http_ok "$GMS_URL/api/graphiql")
  if [[ "$code" == "200" ]]; then
    echo "GraphiQL OK (200)"
    break
  fi
  if (( $(date +%s) > deadline )); then
    echo "Timeout waiting for GraphiQL at $GMS_URL/api/graphiql (last code: $code)" >&2
    exit 2
  fi
  sleep 3
done

echo "Checking GraphQL (/api/graphql) availability..."
while true; do
  code=$(graphql_post "$GMS_URL/api/graphql")
  if [[ "$code" == "200" ]]; then
    echo "GraphQL OK (200)"
    break
  fi
  if (( $(date +%s) > deadline )); then
    echo "Timeout waiting for GraphQL at $GMS_URL/api/graphql (last code: $code)" >&2
    exit 3
  fi
  sleep 3
done

# REST liveness probe. We accept 2xx and 405 (method not allowed) as a sign the endpoint is there.
echo "Pinging REST endpoint for liveness (/entities)..."
code=$(curl -s -o /dev/null -w "%{http_code}" -X GET "$GMS_URL/entities" || true)
if [[ "$code" =~ ^2..$ || "$code" == "405" ]]; then
  echo "REST endpoint responsive ($code)"
else
  echo "REST endpoint check returned code $code (non-fatal)" >&2
fi

echo "All health checks completed."
