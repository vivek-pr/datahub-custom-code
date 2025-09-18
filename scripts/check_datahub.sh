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
CONNECT_TIMEOUT=${CURL_CONNECT_TIMEOUT:-5}
MAX_TIME=${CURL_MAX_TIME:-15}
PORT_FORWARD_PID=""

cleanup() {
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    if kill -0 "$PORT_FORWARD_PID" >/dev/null 2>&1; then
      kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
      wait "$PORT_FORWARD_PID" 2>/dev/null || true
    fi
  fi
}

trap cleanup EXIT
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
  case "$SVC_TYPE" in
    NodePort)
      NODE_PORT=$(kubectl -n "$NS" get svc "$SVC_NAME" -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
      if [[ -z "$NODE_PORT" ]]; then
        echo "Unable to determine nodePort for service '$SVC_NAME'" >&2
        exit 1
      fi
      if [[ -z "${MINIKUBE_IP:-}" ]]; then
        MINIKUBE_IP=$(minikube ip)
      fi
      GMS_URL="http://${MINIKUBE_IP}:${NODE_PORT}"
      DIRECT_CODE=$(curl_code "$GMS_URL/api/health")
      if [[ "${DIRECT_CODE}" == "000" ]]; then
        echo "Direct NodePort access to $GMS_URL failed (curl result 000); falling back to 'minikube service --url' proxy." >&2
        set +e
        URLS=$(minikube service -n "$NS" "$SVC_NAME" --url 2>/dev/null)
        RC=$?
        set -e
        if [[ $RC -eq 0 && -n "$URLS" ]]; then
          GMS_URL=$(echo "$URLS" | head -n1)
        fi
        PROBE_CODE=$(curl_code "$GMS_URL/api/health")
        if [[ "${PROBE_CODE}" == "000" ]]; then
          LOCAL_PORT=${GMS_PORT_FORWARD_PORT:-18080}
          echo "Establishing temporary port-forward to svc/$SVC_NAME on 127.0.0.1:${LOCAL_PORT} for readiness checks." >&2
          kubectl -n "$NS" port-forward svc/"$SVC_NAME" "${LOCAL_PORT}:8080" --address 127.0.0.1 >/tmp/datahub-gms-portfw.log 2>&1 &
          PORT_FORWARD_PID=$!
          sleep 3
          if kill -0 "$PORT_FORWARD_PID" >/dev/null 2>&1; then
            GMS_URL="http://127.0.0.1:${LOCAL_PORT}"
          else
            echo "Port-forward to svc/$SVC_NAME failed; check /tmp/datahub-gms-portfw.log" >&2
          fi
        else
          echo "Using URL from 'minikube service --url': $GMS_URL" >&2
        fi
      fi
      ;;
    LoadBalancer)
      LB_HOST=$(kubectl -n "$NS" get svc "$SVC_NAME" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
      LB_IP=$(kubectl -n "$NS" get svc "$SVC_NAME" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
      PORT=$(kubectl -n "$NS" get svc "$SVC_NAME" -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || echo "")
      HOST=${LB_HOST:-$LB_IP}
      if [[ -z "$HOST" || -z "$PORT" ]]; then
        echo "LoadBalancer service '$SVC_NAME' does not yet have an external address" >&2
        exit 1
      fi
      GMS_URL="http://${HOST}:${PORT}"
      ;;
    ClusterIP)
      echo "Service '$SVC_NAME' is of type ClusterIP. Provide --gms-url that points to a port-forward or set service type to NodePort." >&2
      exit 1
      ;;
    *)
      set +e
      URLS=$(minikube service -n "$NS" "$SVC_NAME" --url 2>/dev/null)
      RC=$?
      set -e
      if [[ $RC -ne 0 || -z "$URLS" ]]; then
        echo "Could not derive GMS URL via minikube service. Ensure the service is exposed: $SVC_NAME in ns $NS" >&2
        exit 1
      fi
      GMS_URL=$(echo "$URLS" | head -n1)
      ;;
  esac
fi

echo "Using GMS URL: $GMS_URL"

start_time=$(date +%s)
deadline=$(( start_time + TIMEOUT ))

curl_code() {
  local url="$1"
  shift
  curl --connect-timeout "$CONNECT_TIMEOUT" --max-time "$MAX_TIME" -fsS -o /dev/null -w "%{http_code}" "$@" "$url" || true
}

http_ok() {
  local url="$1"
  curl_code "$url"
}

graphql_post() {
  local url="$1"
  local body='{"query":"query { __typename }"}'
  curl_code "$url" -H 'Content-Type: application/json' -X POST --data "$body"
}

echo "Checking GraphiQL (/api/graphiql) availability (timeout: ${TIMEOUT}s)..."
attempt=0
while true; do
  attempt=$((attempt + 1))
  code=$(http_ok "$GMS_URL/api/graphiql")
  if [[ "$code" == "200" ]]; then
    elapsed=$(( $(date +%s) - start_time ))
    echo "GraphiQL OK (200) after ${attempt} attempts (${elapsed}s)."
    break
  fi
  if (( $(date +%s) > deadline )); then
    echo "Timeout waiting for GraphiQL at $GMS_URL/api/graphiql (last code: $code, attempts: $attempt)" >&2
    exit 2
  fi
  echo "GraphiQL not ready yet (attempt ${attempt}, code: ${code:-n/a}). Retrying in 3s..."
  sleep 3
done

echo "Checking GraphQL (/api/graphql) availability..."
attempt=0
while true; do
  attempt=$((attempt + 1))
  code=$(graphql_post "$GMS_URL/api/graphql")
  if [[ "$code" == "200" ]]; then
    elapsed=$(( $(date +%s) - start_time ))
    echo "GraphQL OK (200) after ${attempt} attempts (${elapsed}s)."
    break
  fi
  if (( $(date +%s) > deadline )); then
    echo "Timeout waiting for GraphQL at $GMS_URL/api/graphql (last code: $code, attempts: $attempt)" >&2
    exit 3
  fi
  echo "GraphQL not ready yet (attempt ${attempt}, code: ${code:-n/a}). Retrying in 3s..."
  sleep 3
done

# REST liveness probe. We accept 2xx and 405 (method not allowed) as a sign the endpoint is there.
echo "Pinging REST endpoint for liveness (/entities)..."
code=$(curl_code "$GMS_URL/entities" -X GET)
if [[ "$code" =~ ^2..$ || "$code" == "405" ]]; then
  echo "REST endpoint responsive ($code)"
else
  echo "REST endpoint check returned code $code (non-fatal)" >&2
fi

echo "All health checks completed."
