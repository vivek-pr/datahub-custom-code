DC ?= docker compose
PIPELINE_NAME ?= postgres_local_poc

.PHONY: up ingest down logs psql

up: RUN_ID := $(shell date +%s)
up:
	$(DC) up -d --remove-orphans
	$(MAKE) ingest RUN_ID=$(RUN_ID)

ingest: RUN_ID ?= $(shell date +%s)
ingest:
	$(DC) run --rm --entrypoint /bin/sh ingestion -c "set -e; \
	  until curl -sf http://datahub-gms:8080/health > /dev/null; do \
	    echo 'waiting for datahub-gms...'; \
	    sleep 5; \
	  done; \
	  echo 'datahub-gms is healthy, starting ingestion'; \
	  DATAHUB_GMS_URL=http://datahub-gms:8080 DATAHUB_GMS_HOST=datahub-gms DATAHUB_GMS_PORT=8080 DATAHUB_GMS_PROTOCOL=http datahub ingest run -c /ingest/postgres_recipe.yml"

down:
	$(DC) down -v

logs:
	$(DC) logs -f base64-action datahub-gms

psql:
	$(DC) exec postgres psql -U datahub -d postgres
