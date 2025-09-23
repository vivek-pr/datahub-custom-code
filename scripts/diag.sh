#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${1:-tokenize-poc}
CONTEXT=${KUBE_CONTEXT:-}

run_kubectl() {
  if [[ -n "$CONTEXT" ]]; then
    kubectl --context "$CONTEXT" "$@"
  else
    kubectl "$@"
  fi
}

section() {
  echo
  echo "=== $1 ==="
}

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is not available" >&2
  exit 1
fi

section "Cluster info"
run_kubectl get nodes || true

section "Namespaces"
run_kubectl get ns || true

section "Cluster resources (all namespaces)"
run_kubectl get all -A || true

section "Resources in namespace $NAMESPACE"
run_kubectl -n "$NAMESPACE" get all || true

section "Events"
run_kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp || true

section "Pod descriptions"
run_kubectl -n "$NAMESPACE" describe pods || true

section "Action logs"
run_kubectl -n "$NAMESPACE" logs deployment/tokenize-poc-action --tail=200 || true

section "Postgres logs"
run_kubectl -n "$NAMESPACE" logs statefulset/postgresql --tail=200 || true

section "Jobs"
run_kubectl -n "$NAMESPACE" get jobs || true
