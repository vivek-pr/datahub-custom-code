SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

MINIKUBE_PROFILE ?= tokenize-poc
NAMESPACE ?= tokenize-poc
ACTION_IMAGE ?= tokenize-poc-action:latest
ACTION_DEPLOYMENT ?= tokenize-poc-action

VENV ?= .venv
PYTHON ?= python3
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
BLACK := $(VENV)/bin/black

.PHONY: up run down logs test trigger-pg trigger-dbx lint fmt

up:
	./scripts/minikube_up.sh "$(MINIKUBE_PROFILE)" "$(NAMESPACE)" "$(ACTION_IMAGE)"

run:
	kubectl apply -n "$(NAMESPACE)" -f k8s/smoke-job.yaml && kubectl wait --for=condition=complete --timeout=600s job/tokenize-poc-smoke -n "$(NAMESPACE)" && kubectl logs -n "$(NAMESPACE)" job/tokenize-poc-smoke

down:
	./scripts/minikube_down.sh "$(MINIKUBE_PROFILE)" "$(NAMESPACE)"

logs:
	kubectl logs -n "$(NAMESPACE)" -l app="$(ACTION_DEPLOYMENT)" -f

test: $(VENV)/bin/activate
	$(PIP) install -r requirements-dev.txt && $(RUFF) check action tests && $(BLACK) --check action tests && $(PYTEST)

trigger-pg:
	./scripts/trigger.sh --dataset "urn:li:dataset:(urn:li:dataPlatform:postgres,db.schema.customers,PROD)" --columns email,phone --limit 100

trigger-dbx:
	./scripts/trigger.sh --dataset "urn:li:dataset:(urn:li:dataPlatform:databricks,db.schema.customers,PROD)" --columns email,phone --limit 100

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)

lint: $(VENV)/bin/activate
	$(PIP) install -r requirements-dev.txt && $(RUFF) check action tests

fmt: $(VENV)/bin/activate
	$(PIP) install -r requirements-dev.txt && $(BLACK) action tests
