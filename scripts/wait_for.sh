#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <namespace> <resource>" >&2
  exit 1
fi

NAMESPACE=$1
RESOURCE=$2

case "$RESOURCE" in
  deployment/*)
    kubectl -n "$NAMESPACE" rollout status "$RESOURCE" --timeout=300s
    ;;
  service/*)
    kubectl -n "$NAMESPACE" get "$RESOURCE"
    kubectl -n "$NAMESPACE" wait --for=condition=available --timeout=60s deployment/tokenize-poc-action >/dev/null 2>&1 || true
    ;;
  *)
    kubectl -n "$NAMESPACE" wait --for=condition=ready --timeout=300s "$RESOURCE"
    ;;
esac

if [[ "$RESOURCE" == service/* ]]; then
  kubectl run --namespace "$NAMESPACE" curl-check --rm -i --restart=Never --image=curlimages/curl:8.7.1 --command -- sh -c "curl -sf http://tokenize-poc-action:8080/healthz" >/dev/null
fi
