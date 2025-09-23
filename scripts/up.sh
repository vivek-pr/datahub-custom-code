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
POSTGRES_RELEASE=${POSTGRES_RELEASE:-tokenize-poc-postgresql}

log() {
  printf '[%(%Y-%m-%dT%H:%M:%S%z)T] %s\n' -1 "$*"
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
  if [[ -z "$pg_conn_str" ]]; then
    echo "PG_CONN_STR must be set in k8s/secrets.env" >&2
    exit 1
  fi
  kubectl -n "$NAMESPACE" create secret generic tokenize-poc-secrets \
    --from-literal=PG_CONN_STR="$pg_conn_str" \
    --from-literal=DBX_JDBC_URL="$dbx_jdbc_url" \
    --from-literal=TOKEN_SDK_MODE="$token_sdk_mode" \
    --dry-run=client -o yaml | kubectl apply -f -
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
  log "Probing /healthz"
  kubectl -n "$NAMESPACE" run action-health-check --rm -i --restart=Never --image=curlimages/curl:8.7.1 --command -- \
    sh -c "for i in $(seq 1 30); do if curl -sf http://tokenize-poc-action:8080/healthz >/dev/null; then exit 0; fi; sleep 2; done; exit 1" >/dev/null
  log "Action service is healthy"
}

main() {
  require_cmd kubectl
  require_cmd helm
  require_cmd docker
  ensure_image
  start_cluster

  export NAMESPACE IMAGE_REF

  render_and_apply "$ROOT_DIR/k8s/namespace.yaml.tpl"
  apply_secret
  render_and_apply "$ROOT_DIR/k8s/rbac.yaml.tpl"

  helm repo add bitnami https://charts.bitnami.com/bitnami >/dev/null 2>&1 || true
  helm repo update >/dev/null

  log "Deploying Postgres Helm release"
  helm upgrade --install "$POSTGRES_RELEASE" bitnami/postgresql \
    --namespace "$NAMESPACE" \
    --values "$ROOT_DIR/k8s/postgres-values.yaml" \
    --wait

  wait_for_postgres

  load_image_into_cluster

  render_and_apply "$ROOT_DIR/k8s/action-service.yaml.tpl"
  render_and_apply "$ROOT_DIR/k8s/action-deployment.yaml.tpl"

  wait_for_action

  "$ROOT_DIR/scripts/seed_pg.sh" "$NAMESPACE"

  log "Environment ready"
}

main "$@"
