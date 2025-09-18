#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if ! command -v minikube >/dev/null 2>&1; then
  echo "minikube not available, skipping poc smoke test" >&2
  exit 0
fi

KEEP_CLUSTER=0 make poc:smoke

if [ ! -s artifacts/verify/report.json ]; then
  echo "verify report missing" >&2
  exit 1
fi

if [ ! -s artifacts/verify/junit.xml ]; then
  echo "junit report missing" >&2
  exit 1
fi

if [ ! -d artifacts/logs ]; then
  echo "logs directory missing" >&2
  exit 1
fi
