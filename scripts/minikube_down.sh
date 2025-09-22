#!/usr/bin/env bash
set -euo pipefail

PROFILE=${1:-tokenize-poc}
NAMESPACE=${2:-tokenize-poc}

kubectl delete namespace "$NAMESPACE" --ignore-not-found
minikube -p "$PROFILE" delete >/dev/null 2>&1 || true
