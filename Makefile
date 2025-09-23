SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

ROOT_DIR := $(CURDIR)
IMAGE_NAME ?= tokenize-poc/action
IMAGE_TAG ?= local
IMAGE_REF := $(IMAGE_NAME):$(IMAGE_TAG)
K8S_NS ?= tokenize-poc
CLUSTER ?= minikube
MINIKUBE_PROFILE ?= tokenize-poc
MINIKUBE_DRIVER ?= docker
KIND_CLUSTER_NAME ?= tokenize-poc
POSTGRES_RELEASE ?= tokenize-poc-postgresql

PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
BLACK := $(VENV)/bin/black

.PHONY: build push up run down logs diag test fmt lint trigger-pg trigger-dbx ci clean

build:
	DOCKER_BUILDKIT=1 docker build \
		--file docker/action.Dockerfile \
		--tag $(IMAGE_REF) \
		.

push:
	docker push $(IMAGE_REF)

up:
	CLUSTER=$(CLUSTER) \
	IMAGE_NAME=$(IMAGE_NAME) \
	IMAGE_TAG=$(IMAGE_TAG) \
	IMAGE_REF=$(IMAGE_REF) \
	K8S_NS=$(K8S_NS) \
	MINIKUBE_PROFILE=$(MINIKUBE_PROFILE) \
	MINIKUBE_DRIVER=$(MINIKUBE_DRIVER) \
	KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) \
	POSTGRES_RELEASE=$(POSTGRES_RELEASE) \
	./scripts/up.sh

run:
	$(PYTHON) ./scripts/run_e2e.py --namespace $(K8S_NS)

down:
	CLUSTER=$(CLUSTER) \
	K8S_NS=$(K8S_NS) \
	MINIKUBE_PROFILE=$(MINIKUBE_PROFILE) \
	KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) \
	POSTGRES_RELEASE=$(POSTGRES_RELEASE) \
	./scripts/down.sh

logs:
	kubectl -n $(K8S_NS) logs deployment/tokenize-poc-action -f

diag:
	./scripts/diag.sh $(K8S_NS)

trigger-pg:
	./scripts/trigger.sh --namespace $(K8S_NS) --dataset "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.schema.customers,PROD)" --columns email,phone --limit 100

trigger-dbx:
	./scripts/trigger.sh --namespace $(K8S_NS) --dataset "urn:li:dataset:(urn:li:dataPlatform:databricks,tokenize.schema.customers,PROD)" --columns email,phone --limit 100

$(VENV)/bin/python:
	$(PYTHON) -m venv $(VENV)

lint: $(VENV)/bin/python
	$(PIP) install -r requirements-dev.txt
	$(RUFF) check action tests

fmt: $(VENV)/bin/python
	$(PIP) install -r requirements-dev.txt
	$(BLACK) action tests

clean:
	rm -rf $(VENV)

test: $(VENV)/bin/python
	$(PIP) install -r requirements-dev.txt
	$(RUFF) check action tests
	$(BLACK) --check action tests
	$(PYTEST)

ci:
	$(MAKE) build CLUSTER=kind IMAGE_TAG=$(IMAGE_TAG)
	$(MAKE) up CLUSTER=kind IMAGE_TAG=$(IMAGE_TAG) K8S_NS=$(K8S_NS)
	$(MAKE) run K8S_NS=$(K8S_NS)
	$(MAKE) down CLUSTER=kind K8S_NS=$(K8S_NS)
