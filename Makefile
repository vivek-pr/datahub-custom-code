SHELL := /bin/bash

# Configurable parameters
NS ?= datahub
RELEASE_DATAHUB ?= datahub
RELEASE_PREREQ ?= prerequisites
VALUES_DIR ?= infra/helm
PREREQ_VALUES ?= $(VALUES_DIR)/prerequisites-values.yaml
DATAHUB_VALUES ?= $(VALUES_DIR)/datahub-values.yaml

# Minikube defaults (override via env or CLI)
MK_CPUS ?= 4
MK_MEMORY ?= 7837
MK_DISK ?= 40g

.PHONY: mk-up mk-status helm-repo datahub-install datahub-uninstall datahub-status datahub-portfw datahub-portfw-stop datahub-test-integration datahub-test-e2e

mk-up:
	@echo "Starting Minikube with $(MK_CPUS) CPUs, $(MK_MEMORY)MB RAM, $(MK_DISK) disk..."
	minikube start --cpus=$(MK_CPUS) --memory=$(MK_MEMORY) --disk-size=$(MK_DISK)
	@echo "Minikube started. Kubernetes context: $$(kubectl config current-context)"

mk-status:
	minikube status

helm-repo:
	@echo "Ensuring acryldata Helm repo is added..."
	@if ! helm repo list | awk '{print $$1}' | grep -q "^acryldata$$"; then \
		echo "Adding acryldata Helm repo..."; \
		helm repo add acryldata https://helm.acryldata.io || true; \
	fi
	@echo "Updating Helm repos..."
	helm repo update

datahub-install: helm-repo
	@echo "Creating namespace $(NS) if not exists..."
	kubectl get ns $(NS) >/dev/null 2>&1 || kubectl create ns $(NS)
	@echo "Installing/upgrading prerequisites (Kafka, Zookeeper, Elasticsearch, DB)..."
	helm upgrade --install $(RELEASE_PREREQ) acryldata/prerequisites -n $(NS) -f $(PREREQ_VALUES)
	@echo "Installing/upgrading DataHub..."
	helm upgrade --install $(RELEASE_DATAHUB) acryldata/datahub -n $(NS) -f $(DATAHUB_VALUES)
	@echo "Waiting for DataHub pods to be ready..."
	kubectl wait --for=condition=Ready pods --all -n $(NS) --timeout=10m || true
	@$(MAKE) datahub-status

datahub-uninstall:
	@echo "Uninstalling DataHub release $(RELEASE_DATAHUB) from namespace $(NS)..."
	-helm uninstall $(RELEASE_DATAHUB) -n $(NS)
	@echo "Uninstalling prerequisites release $(RELEASE_PREREQ) from namespace $(NS)..."
	-helm uninstall $(RELEASE_PREREQ) -n $(NS)
	@echo "Note: PersistentVolumes may remain; delete manually if desired."

datahub-status:
	@echo "Pods in namespace $(NS):"
	kubectl get pods -n $(NS) -o wide
	@echo "Services in namespace $(NS):"
	kubectl get svc -n $(NS)
	@echo "Running health checks..."
	bash scripts/check_datahub.sh --namespace $(NS)

# Port-forward common services for local access
datahub-portfw:
	@mkdir -p .portfw
	@echo "Starting port-forwards for GMS (8080) and Frontend (9002) in namespace $(NS)..."
	@# GMS
	- (nohup kubectl -n $(NS) port-forward svc/$(RELEASE_DATAHUB)-datahub-gms 8080:8080 > .portfw/gms.log 2>&1 & echo $$! > .portfw/gms.pid)
	@# Frontend
	- (nohup kubectl -n $(NS) port-forward svc/$(RELEASE_DATAHUB)-datahub-frontend 9002:9002 > .portfw/frontend.log 2>&1 & echo $$! > .portfw/frontend.pid)
	@echo "Port-forwards active. PIDs: $$(cat .portfw/gms.pid 2>/dev/null || echo N/A), $$(cat .portfw/frontend.pid 2>/dev/null || echo N/A)"
	@echo "Open UI: http://localhost:9002  |  GraphQL: http://localhost:8080/api/graphiql"

datahub-portfw-stop:
	@echo "Stopping port-forwards..."
	-@[ -f .portfw/gms.pid ] && kill $$(cat .portfw/gms.pid) && rm -f .portfw/gms.pid || true
	-@[ -f .portfw/frontend.pid ] && kill $$(cat .portfw/frontend.pid) && rm -f .portfw/frontend.pid || true
	@echo "Stopped."

datahub-test-integration:
	python3 -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r scripts/requirements.txt && \
	python scripts/test_datahub_integration.py --namespace $(NS)

datahub-test-e2e:
	python3 -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r scripts/requirements.txt && \
	python scripts/test_datahub_e2e.py --namespace $(NS)

# Backward-compatible aliases (colon targets). The ':' in target names is escaped.
mk\:up:
	@$(MAKE) mk-up
mk\:status:
	@$(MAKE) mk-status
helm\:repo:
	@$(MAKE) helm-repo
datahub\:install:
	@$(MAKE) datahub-install
datahub\:uninstall:
	@$(MAKE) datahub-uninstall
datahub\:status:
	@$(MAKE) datahub-status
datahub\:portfw:
	@$(MAKE) datahub-portfw
datahub\:portfw\:stop:
	@$(MAKE) datahub-portfw-stop
datahub\:test\:integration:
	@$(MAKE) datahub-test-integration
datahub\:test\:e2e:
	@$(MAKE) datahub-test-e2e
