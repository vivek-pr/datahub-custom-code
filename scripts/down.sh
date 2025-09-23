#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
CLUSTER=${CLUSTER:-minikube}
NAMESPACE=${K8S_NS:-tokenize-poc}
MINIKUBE_PROFILE=${MINIKUBE_PROFILE:-tokenize-poc}
KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-tokenize-poc}
POSTGRES_RELEASE=${POSTGRES_RELEASE:-tokenize-poc-postgresql}

log() {
  printf '[%(%Y-%m-%dT%H:%M:%S%z)T] %s\n' -1 "$*"
}

teardown_namespace() {
  local context=$1
  if command -v kubectl >/dev/null 2>&1; then
    log "Deleting namespace $NAMESPACE (if present)"
    kubectl --context "$context" delete namespace "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
  fi
}

teardown_helm() {
  local context=$1
  if command -v helm >/dev/null 2>&1; then
    helm --kube-context "$context" uninstall "$POSTGRES_RELEASE" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
  fi
}

main() {
  case "$CLUSTER" in
    minikube)
      local context="$MINIKUBE_PROFILE"
      teardown_helm "$context"
      teardown_namespace "$context"
      if command -v minikube >/dev/null 2>&1; then
        log "Deleting minikube profile $MINIKUBE_PROFILE"
        minikube -p "$MINIKUBE_PROFILE" delete >/dev/null 2>&1 || true
      fi
      ;;
    kind)
      local context="kind-$KIND_CLUSTER_NAME"
      teardown_helm "$context"
      teardown_namespace "$context"
      if command -v kind >/dev/null 2>&1; then
        log "Deleting kind cluster $KIND_CLUSTER_NAME"
        kind delete cluster --name "$KIND_CLUSTER_NAME" >/dev/null 2>&1 || true
      fi
      ;;
    *)
      log "Unknown CLUSTER '$CLUSTER'; nothing to do"
      ;;
  esac
  log "Teardown complete"
}

main "$@"
