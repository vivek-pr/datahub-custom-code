SHELL := /bin/sh
.ONESHELL:
.SHELLFLAGS := -eu -c

ROOT_DIR := $(CURDIR)
IMAGE_NAME ?= tokenize-poc/action
IMAGE_TAG ?= local
IMAGE_REF := $(IMAGE_NAME):$(IMAGE_TAG)
K8S_NS ?= tokenize-poc
PG_RELEASE ?= tokenize-poc-postgresql
PG_CHART ?= oci://registry-1.docker.io/bitnamicharts/postgresql
PG_VALUES ?= k8s/postgres-values.yaml
CLUSTER ?= minikube
MINIKUBE_PROFILE ?= tokenize-poc
MINIKUBE_DRIVER ?= docker
KIND_CLUSTER_NAME ?= tokenize-poc

PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
BLACK := $(VENV)/bin/black
DATAHUB := $(VENV)/bin/datahub

DATASET_NAME ?= public.customers
DATASET_PLATFORM ?= postgres
STATUS_TIMEOUT ?= 600
ARTIFACT_DIR := artifacts

.PHONY: build up down reset-pg ingest ingest-pg ingest-dbx seed-pg trigger-ui wait-status verify-idempotent e2e test fmt lint clean

build:
	DOCKER_BUILDKIT=1 docker build \
		--file docker/action.Dockerfile \
		--tag "$(IMAGE_REF)" \
		.

up:
	$(MAKE) reset-pg
	CLUSTER="$(CLUSTER)" \
	IMAGE_NAME="$(IMAGE_NAME)" \
	IMAGE_TAG="$(IMAGE_TAG)" \
	IMAGE_REF="$(IMAGE_REF)" \
	K8S_NS="$(K8S_NS)" \
	MINIKUBE_PROFILE="$(MINIKUBE_PROFILE)" \
	MINIKUBE_DRIVER="$(MINIKUBE_DRIVER)" \
	KIND_CLUSTER_NAME="$(KIND_CLUSTER_NAME)" \
	PG_RELEASE="$(PG_RELEASE)" \
	PG_CHART="$(PG_CHART)" \
	PG_VALUES="$(PG_VALUES)" \
	./scripts/up.sh

down:
	CLUSTER="$(CLUSTER)" \
	K8S_NS="$(K8S_NS)" \
	MINIKUBE_PROFILE="$(MINIKUBE_PROFILE)" \
	KIND_CLUSTER_NAME="$(KIND_CLUSTER_NAME)" \
	PG_RELEASE="$(PG_RELEASE)" \
	./scripts/down.sh

reset-pg:
	chmod +x scripts/helm_sanitize_pg.sh
	printf '%s\n' ">> Sanitizing Postgres Helm leftovers in ns '$(K8S_NS)'"
	./scripts/helm_sanitize_pg.sh "$(K8S_NS)" "postgresql" "$(PG_RELEASE)"

$(VENV)/bin/python:
	"$(PYTHON)" -m venv "$(VENV)"
	"$(PIP)" install -r requirements-dev.txt

lint: $(VENV)/bin/python
	"$(RUFF)" check action tests

fmt: $(VENV)/bin/python
	"$(BLACK)" action tests

clean:
	rm -rf "$(VENV)" "$(ARTIFACT_DIR)"

seed-pg:
	./scripts/seed_pg.sh "$(K8S_NS)"

ingest-pg: $(VENV)/bin/python
	mkdir -p "$(ARTIFACT_DIR)"
	. k8s/secrets.env
	export PG_CONN_STR="$$PG_CONN_STR"
	eval "$$($(PYTHON) scripts/pg_env.py)"
	if [ -z "$$DATAHUB_GMS" ]; then echo "DATAHUB_GMS must be set" >&2; exit 1; fi
	kubectl -n "$(K8S_NS)" port-forward svc/postgresql 15432:5432 >/tmp/postgres-portforward.log 2>&1 &
	PF_PID=$$!
	trap 'kill $$PF_PID >/dev/null 2>&1 || true' EXIT
	sleep 5
	INGEST_PG_HOST=127.0.0.1 INGEST_PG_PORT=15432 \
	INGEST_PG_USERNAME="$$INGEST_PG_USERNAME" \
	INGEST_PG_PASSWORD="$$INGEST_PG_PASSWORD" \
	INGEST_PG_DATABASE="$$INGEST_PG_DATABASE" \
	DATAHUB_GMS="$$DATAHUB_GMS" DATAHUB_TOKEN="$$DATAHUB_TOKEN" \
	"$(DATAHUB)" ingest -c ingestion/postgres.yml
	kill $$PF_PID >/dev/null 2>&1 || true
	trap - EXIT

ingest-dbx: $(VENV)/bin/python
	mkdir -p "$(ARTIFACT_DIR)"
	. k8s/secrets.env
	if [ -z "$$DBX_JDBC_URL" ]; then \
		printf '%s\n' "DBX_JDBC_URL not configured; skipping Databricks ingestion"; \
		exit 0; \
	fi
	export DBX_JDBC_URL="$$DBX_JDBC_URL"
	eval "$$($(PYTHON) scripts/dbx_env.py)"
	INGEST_DBX_SERVER="$$INGEST_DBX_SERVER" \
	INGEST_DBX_HTTP_PATH="$$INGEST_DBX_HTTP_PATH" \
	INGEST_DBX_TOKEN="$$INGEST_DBX_TOKEN" \
	INGEST_DBX_CATALOG="$$DBX_CATALOG" \
	INGEST_DBX_SCHEMA="$$DBX_SCHEMA" \
	INGEST_DBX_TABLE="$$DBX_TABLE" \
	DATAHUB_GMS="$$DATAHUB_GMS" DATAHUB_TOKEN="$$DATAHUB_TOKEN" \
	"$(DATAHUB)" ingest -c ingestion/databricks.yml

ingest: ingest-pg ingest-dbx

trigger-ui:
	. k8s/secrets.env
	DATASET_URN=$${DATASET_URN:-$$($(PYTHON) scripts/find_dataset_urn.py $(DATASET_NAME) --platform $(DATASET_PLATFORM))}
	DATAHUB_GMS="$$DATAHUB_GMS" DATAHUB_TOKEN="$$DATAHUB_TOKEN" scripts/add_tag.sh "$$DATASET_URN"

wait-status:
	mkdir -p "$(ARTIFACT_DIR)"
	. k8s/secrets.env
	DATASET_URN=$${DATASET_URN:-$$($(PYTHON) scripts/find_dataset_urn.py $(DATASET_NAME) --platform $(DATASET_PLATFORM))}
	DATAHUB_GMS="$$DATAHUB_GMS" DATAHUB_TOKEN="$$DATAHUB_TOKEN" scripts/poll_status.sh "$$DATASET_URN" "$(STATUS_TIMEOUT)" > "$(ARTIFACT_DIR)/last_status.json"
	jq -e 'select(.rows_updated > 0)' "$(ARTIFACT_DIR)/last_status.json" >/dev/null

verify-idempotent:
	mkdir -p "$(ARTIFACT_DIR)"
	. k8s/secrets.env
	DATASET_URN=$${DATASET_URN:-$$($(PYTHON) scripts/find_dataset_urn.py $(DATASET_NAME) --platform $(DATASET_PLATFORM))}
	DATAHUB_GMS="$$DATAHUB_GMS" DATAHUB_TOKEN="$$DATAHUB_TOKEN" scripts/add_tag.sh "$$DATASET_URN"
	DATAHUB_GMS="$$DATAHUB_GMS" DATAHUB_TOKEN="$$DATAHUB_TOKEN" scripts/poll_status.sh "$$DATASET_URN" "$(STATUS_TIMEOUT)" > "$(ARTIFACT_DIR)/idempotent_status.json"
	jq -e 'select(.rows_updated == 0)' "$(ARTIFACT_DIR)/idempotent_status.json" >/dev/null
	echo "Idempotency verified"

e2e: build up ingest seed-pg trigger-ui wait-status verify-idempotent

TEST_FLAGS ?=

test: $(VENV)/bin/python
	"$(RUFF)" check action tests
	"$(BLACK)" --check action tests
	./scripts/verify_printf.sh
	"$(PYTEST)" $(TEST_FLAGS)
