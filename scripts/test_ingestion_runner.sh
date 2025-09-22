#!/usr/bin/env bash
set -euo pipefail

log_file="$(mktemp -t ingestion-run-XXXX.log)"
trap 'rm -f "$log_file"' EXIT

echo "[test] Running ingestion container via docker compose"
if docker compose run --rm ingestion >"$log_file" 2>&1; then
  if [ ! -s "$log_file" ]; then
    echo "[test] Ingestion logs were empty" >&2
    cat "$log_file" >&2 || true
    exit 1
  fi
  echo "[test] Ingestion completed successfully with output:"
  cat "$log_file"
else
  status=$?
  echo "[test] Ingestion container exited with status $status" >&2
  cat "$log_file" >&2 || true
  exit "$status"
fi
