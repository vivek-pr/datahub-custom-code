#!/bin/sh
set -euo pipefail

NS="${1:-tokenize-poc}"
OLD_RELEASE="${2:-postgresql}"
NEW_RELEASE="${3:-tokenize-poc-postgresql}"

if ! command -v helm >/dev/null 2>&1 || ! command -v kubectl >/dev/null 2>&1; then
  printf '%s\n' "helm_sanitize_pg: kubectl/helm not available; skipping cleanup" >&2
  exit 0
fi

if ! kubectl get namespace "$NS" >/dev/null 2>&1; then
  printf '%s\n' "helm_sanitize_pg: namespace '$NS' not reachable yet; skipping cleanup" >&2
  exit 0
fi

if helm status "$OLD_RELEASE" -n "$NS" >/dev/null 2>&1; then
  printf '%s\n' "Found old release '$OLD_RELEASE' in ns '$NS' -> uninstalling"
  if ! helm uninstall "$OLD_RELEASE" -n "$NS"; then
    printf '%s\n' "Warning: failed to uninstall stale release '$OLD_RELEASE'" >&2
  fi
fi

CANDIDATES="
networkpolicy/postgresql
svc/postgresql
secret/postgresql
configmap/postgresql
role/postgresql
rolebinding/postgresql
serviceaccount/postgresql
"

for r in $CANDIDATES; do
  if kubectl get -n "$NS" "$r" >/dev/null 2>&1; then
    ANNO_NAME=$(kubectl get -n "$NS" "$r" -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-name}' 2>/dev/null || printf '')
    ANNO_NS=$(kubectl get -n "$NS" "$r" -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-namespace}' 2>/dev/null || printf '')
    if [ "${ANNO_NAME:-}" != "$NEW_RELEASE" ] || [ "${ANNO_NS:-}" != "$NS" ]; then
      printf '%s\n' "Deleting conflicting resource $r (release-name='${ANNO_NAME:-none}', ns='${ANNO_NS:-none}')"
      kubectl delete -n "$NS" "$r" --wait=true >/dev/null 2>&1 || kubectl delete -n "$NS" "$r" --wait=false || true
    fi
  fi
done
