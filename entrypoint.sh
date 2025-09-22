#!/bin/sh
# shellcheck shell=sh

if ! set -euo pipefail 2>/dev/null; then
  set -eu
  # shellcheck disable=SC3040
  set -o pipefail 2>/dev/null || true
fi

log() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

random_sleep() {
  python3 - "$1" <<'PY'
import random
import sys
base = float(sys.argv[1])
# Add up to 1 second of jitter
sleep_for = base + random.random()
print(f"{sleep_for:.2f}")
PY
}

retry() {
  desc=$1
  shift
  attempt=1
  delay=${RETRY_INITIAL_DELAY:-2}
  max_attempts=${RETRY_MAX_ATTEMPTS:-20}
  backoff=${RETRY_BACKOFF_MULTIPLIER:-2}
  max_delay=${RETRY_MAX_DELAY:-30}

  while [ "$attempt" -le "$max_attempts" ]; do
    if "$@"; then
      log "$desc succeeded (attempt $attempt)"
      return 0
    fi

    if [ "$attempt" -eq "$max_attempts" ]; then
      log "$desc failed after $attempt attempts"
      return 1
    fi

    attempt=$((attempt + 1))
    sleep_for=$(random_sleep "$delay")
    log "$desc retrying in ${sleep_for}s (attempt $attempt of $max_attempts)"
    sleep "$sleep_for"

    if [ "$delay" -lt "$max_delay" ]; then
      delay=$((delay * backoff))
      if [ "$delay" -gt "$max_delay" ]; then
        delay=$max_delay
      fi
    fi
  done

  return 1
}

check_gms() {
  curl -fsS --max-time "${GMS_HEALTH_TIMEOUT:-5}" "$DATAHUB_GMS_URI/api/health" >/tmp/gms-health.json
}

check_db() {
  python3 - "$DB_HOST" "$DB_PORT" <<'PY'
import socket
import sys
host = sys.argv[1]
port = int(sys.argv[2])
with socket.create_connection((host, port), timeout=5):
    pass
PY
}

main() {
  DATAHUB_GMS_URI=${DATAHUB_GMS_URI:-http://datahub-gms:8080}
  export DATAHUB_GMS_URI
  RECIPE_FILE=${RECIPE_FILE:-/workspace/ingest/recipe.yml}
  DB_HOST=${DB_HOST:-postgres}
  DB_PORT=${DB_PORT:-5432}
  STARTUP_DELAY=${STARTUP_DELAY:-5}

  log "Starting ingestion runner"
  log "Using recipe file: $RECIPE_FILE"
  log "Waiting for DataHub GMS health at $DATAHUB_GMS_URI/api/health"
  retry "DataHub GMS health check" check_gms
  if [ -f /tmp/gms-health.json ]; then
    log "DataHub GMS health payload: $(tr -d '\n' </tmp/gms-health.json)"
    rm -f /tmp/gms-health.json
  fi

  log "Waiting for database availability at $DB_HOST:$DB_PORT"
  retry "Database connectivity check" check_db

  case "${STARTUP_DELAY}" in
    ''|0)
      ;;
    *)
      log "Startup delay requested: sleeping ${STARTUP_DELAY}s"
      sleep "$STARTUP_DELAY"
      ;;
  esac

  if [ ! -f "$RECIPE_FILE" ]; then
    log "Recipe file not found: $RECIPE_FILE"
    exit 1
  fi

  log "Health checks passed. Executing ingestion"
  exec datahub ingest -c "$RECIPE_FILE"
}

main "$@"
