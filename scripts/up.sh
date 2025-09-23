#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
CLUSTER=${CLUSTER:-minikube}
NAMESPACE=${K8S_NS:-tokenize-poc}
IMAGE_NAME=${IMAGE_NAME:-tokenize-poc/action}
IMAGE_TAG=${IMAGE_TAG:-local}
IMAGE_REF=${IMAGE_REF:-$IMAGE_NAME:$IMAGE_TAG}
MINIKUBE_PROFILE=${MINIKUBE_PROFILE:-tokenize-poc}
MINIKUBE_DRIVER=${MINIKUBE_DRIVER:-docker}
KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-tokenize-poc}
PG_RELEASE=${PG_RELEASE:-tokenize-poc-postgresql}
PG_CHART=${PG_CHART:-oci://registry-1.docker.io/bitnamicharts/postgresql}
PG_VALUES=${PG_VALUES:-$ROOT_DIR/k8s/postgres-values.yaml}
if [[ "${PG_VALUES}" != /* ]]; then
  PG_VALUES="$ROOT_DIR/${PG_VALUES}"
fi

log() {
  local now
  now=$(date +%Y-%m-%dT%H:%M:%S%z)
  printf '[%s] %s\n' "$now" "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command '$1' not found. Please install it and retry." >&2
    exit 1
  fi
}

ensure_image() {
  if ! docker image inspect "$IMAGE_REF" >/dev/null 2>&1; then
    echo "Docker image $IMAGE_REF not found. Run 'make build' first." >&2
    exit 1
  fi
}

render_and_apply() {
  local template=$1
  python - "$template" <<'PYIN' | kubectl apply -f -
import os
import string
import sys

template_path = sys.argv[1]
with open(template_path, "r", encoding="utf-8") as handle:
    content = handle.read()
print(string.Template(content).safe_substitute(os.environ))
PYIN
}

apply_secret() {
  local secrets_file="$ROOT_DIR/k8s/secrets.env"
  if [[ ! -f "$secrets_file" ]]; then
    cp "$ROOT_DIR/k8s/secrets.example.env" "$secrets_file"
    cat >&2 <<'MSG'
Created k8s/secrets.env from example template.
Continuing with defaults; edit k8s/secrets.env to customise credentials.
MSG
  fi
  # shellcheck disable=SC1090
  source "$secrets_file"
  local pg_conn_str="${PG_CONN_STR:-}"
  local dbx_jdbc_url="${DBX_JDBC_URL:-}"
  local token_sdk_mode="${TOKEN_SDK_MODE:-dummy}"
  local datahub_gms="${DATAHUB_GMS:-}"
  local datahub_token="${DATAHUB_TOKEN:-}"
  local dbx_catalog="${DBX_CATALOG:-}"
  local dbx_schema="${DBX_SCHEMA:-}"
  local dbx_table="${DBX_TABLE:-}"
  if [[ -z "$pg_conn_str" ]]; then
    echo "PG_CONN_STR must be set in k8s/secrets.env" >&2
    exit 1
  fi
  if [[ -z "$datahub_gms" ]]; then
    echo "DATAHUB_GMS must be set in k8s/secrets.env" >&2
    exit 1
  fi
  python - "$NAMESPACE" "$pg_conn_str" "$dbx_jdbc_url" "$token_sdk_mode" <<'PYIN' | kubectl apply -f -
import json
import sys
import os

namespace, pg_conn, dbx_jdbc, token_mode = sys.argv[1:5]
datahub_gms = os.environ.get("DATAHUB_GMS", "")
datahub_token = os.environ.get("DATAHUB_TOKEN", "")
dbx_catalog = os.environ.get("DBX_CATALOG", "")
dbx_schema = os.environ.get("DBX_SCHEMA", "")
dbx_table = os.environ.get("DBX_TABLE", "")

manifest = {
    "apiVersion": "v1",
    "kind": "Secret",
    "metadata": {
        "name": "tokenize-poc-secrets",
        "namespace": namespace,
    },
    "type": "Opaque",
    "stringData": {
        "PG_CONN_STR": pg_conn,
        "DBX_JDBC_URL": dbx_jdbc,
        "TOKEN_SDK_MODE": token_mode,
        "DATAHUB_GMS": datahub_gms,
        "DATAHUB_TOKEN": datahub_token,
        "DBX_CATALOG": dbx_catalog,
        "DBX_SCHEMA": dbx_schema,
        "DBX_TABLE": dbx_table,
    },
}

json.dump(manifest, sys.stdout)
PYIN
}

start_cluster() {
  case "$CLUSTER" in
    minikube)
      require_cmd minikube
      if ! minikube -p "$MINIKUBE_PROFILE" status >/dev/null 2>&1; then
        log "Starting minikube profile $MINIKUBE_PROFILE"
        minikube start -p "$MINIKUBE_PROFILE" --driver="$MINIKUBE_DRIVER" --memory=8192 --cpus=4
      else
        log "Reusing existing minikube profile $MINIKUBE_PROFILE"
      fi
      kubectl config use-context "$MINIKUBE_PROFILE" >/dev/null
      ;;
    kind)
      require_cmd kind
      if ! kind get clusters | grep -qx "$KIND_CLUSTER_NAME"; then
        log "Creating kind cluster $KIND_CLUSTER_NAME"
        kind create cluster --name "$KIND_CLUSTER_NAME" --wait 120s
      else
        log "Reusing existing kind cluster $KIND_CLUSTER_NAME"
      fi
      kubectl config use-context "kind-$KIND_CLUSTER_NAME" >/dev/null
      ;;
    *)
      echo "Unsupported CLUSTER value '$CLUSTER'. Use 'minikube' or 'kind'." >&2
      exit 1
      ;;
  esac
}

load_image_into_cluster() {
  case "$CLUSTER" in
    minikube)
      log "Loading Docker image $IMAGE_REF into minikube"
      minikube -p "$MINIKUBE_PROFILE" image load "$IMAGE_REF"
      ;;
    kind)
      log "Loading Docker image $IMAGE_REF into kind"
      kind load docker-image "$IMAGE_REF" --name "$KIND_CLUSTER_NAME"
      ;;
  esac
}

wait_for_postgres() {
  log "Waiting for Postgres to become ready"
  kubectl -n "$NAMESPACE" rollout status statefulset/postgresql --timeout=300s >/dev/null 2>&1 || true
  local deadline=$((SECONDS + 300))
  while (( SECONDS < deadline )); do
    local pod
    pod=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -z "$pod" ]]; then
      sleep 5
      continue
    fi
    if kubectl -n "$NAMESPACE" exec "$pod" -- pg_isready -d "postgresql://tokenize:tokenize@localhost:5432/tokenize" >/dev/null 2>&1; then
      log "Postgres is ready"
      return
    fi
    sleep 5
  done
  echo "Timed out waiting for Postgres readiness" >&2
  exit 1
}

wait_for_action() {
  log "Waiting for action deployment rollout"
  kubectl -n "$NAMESPACE" rollout status deployment/tokenize-poc-action --timeout=300s

  log "Waiting for tokenize-poc-action service endpoints"
  local endpoint_ips=""
  for _ in $(seq 1 40); do
    endpoint_ips=$(kubectl -n "$NAMESPACE" get endpoints tokenize-poc-action -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)
    if [[ -n "$endpoint_ips" ]]; then
      break
    fi
    sleep 3
  done

  if [[ -z "$endpoint_ips" ]]; then
    echo "Service tokenize-poc-action has no ready endpoints" >&2
    kubectl -n "$NAMESPACE" get svc tokenize-poc-action -o wide || true
    kubectl -n "$NAMESPACE" get endpoints tokenize-poc-action -o yaml || true
    kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=tokenize-poc-action -o wide || true
    exit 2
  fi

  log "Probing /healthz"
  if ! kubectl -n "$NAMESPACE" run action-health-check \
    --image=curlimages/curl:8.8.0 \
    --rm --restart=Never --attach -- \
    curl -fsS -m 5 "http://tokenize-poc-action.${NAMESPACE}.svc.cluster.local:8080/healthz" >/dev/null; then
    echo "Action /healthz probe failed" >&2
    kubectl -n "$NAMESPACE" logs deployment/tokenize-poc-action --tail=200 || true
    kubectl -n "$NAMESPACE" describe svc tokenize-poc-action || true
    kubectl -n "$NAMESPACE" get endpoints tokenize-poc-action -o yaml || true
    exit 2
  fi

  log "Action service is healthy"
}

main() {
  require_cmd kubectl
  require_cmd helm
  require_cmd docker
  ensure_image
  start_cluster

  log "Sanitizing Postgres Helm leftovers"
  "$ROOT_DIR/scripts/helm_sanitize_pg.sh" "$NAMESPACE" "postgresql" "$PG_RELEASE"

  export NAMESPACE IMAGE_REF

  render_and_apply "$ROOT_DIR/k8s/namespace.yaml.tpl"
  apply_secret
  render_and_apply "$ROOT_DIR/k8s/rbac.yaml.tpl"

  log "Deploying Postgres Helm release"
  helm upgrade --install "$PG_RELEASE" "$PG_CHART" \
    --namespace "$NAMESPACE" \
    --values "$PG_VALUES" \
    --create-namespace \
    --wait \
    --timeout 10m \
    --atomic

  wait_for_postgres

  load_image_into_cluster

  render_and_apply "$ROOT_DIR/k8s/action-service.yaml.tpl"
  render_and_apply "$ROOT_DIR/k8s/networkpolicy-allow-action.yaml.tpl"
  render_and_apply "$ROOT_DIR/k8s/action-deployment.yaml.tpl"

  wait_for_action

  log "Environment ready"
}

main "$@"
