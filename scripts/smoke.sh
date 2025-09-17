#!/usr/bin/env bash
set -euo pipefail

echo "Running smoke checks..."

# Ensure required docs exist
for f in docs/00_overview.md docs/decisions.md; do
  if [[ ! -s "$f" ]]; then
    echo "ERROR: missing or empty $f" >&2
    exit 1
  fi
done

# Ensure pre-commit config exists
if [[ ! -s .pre-commit-config.yaml ]]; then
  echo "ERROR: missing .pre-commit-config.yaml" >&2
  exit 1
fi

echo "Smoke checks passed."
