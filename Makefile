SHELL := /bin/bash

# Configurable parameters
NS ?= datahub
RELEASE_DATAHUB ?= datahub
RELEASE_PREREQ ?= prerequisites
VALUES_DIR ?= infra/helm
PREREQ_VALUES ?= $(VALUES_DIR)/prerequisites-values.yaml
DATAHUB_VALUES ?= $(VALUES_DIR)/datahub-values.yaml

# Helm sources
HELM_REPO_NAME ?= acryldata
HELM_REPO_URL ?= https://helm.acryldata.io
HELM_REPO_CHART_DATAHUB ?= $(HELM_REPO_NAME)/datahub
HELM_REPO_CHART_PREREQ ?= $(HELM_REPO_NAME)/prerequisites

# Fallback to local charts from GitHub if repo is unreachable
HELM_CHART_REPO_DIR ?= .helm-charts/acryldata/datahub-helm
HELM_CHART_REF ?= master
HELM_CHART_DATAHUB_PATH ?= $(HELM_CHART_REPO_DIR)/charts/datahub
HELM_CHART_PREREQ_PATH ?= $(HELM_CHART_REPO_DIR)/charts/prerequisites

# Minikube defaults (override via env or CLI)
MK_CPUS ?= 4
MK_MEMORY ?= 8192
MK_DISK ?= 40g

.PHONY: mk-up mk-status helm-repo datahub-install datahub-uninstall datahub-status datahub-portfw datahub-portfw-stop datahub-test-integration datahub-test-e2e pg-up pg-load pg-ingest pg-purge classifier-run classifier\:run

mk-up:
	@echo "Starting Minikube with $(MK_CPUS) CPUs, $(MK_MEMORY)MB RAM, $(MK_DISK) disk..."
	minikube start --cpus=$(MK_CPUS) --memory=$(MK_MEMORY) --disk-size=$(MK_DISK)
	@echo "Minikube started. Kubernetes context: $$(kubectl config current-context)"

mk-status:
	minikube status

helm-repo:
	@echo "Ensuring acryldata Helm repo is added..."
	@if ! helm repo list | awk '{print $$1}' | grep -q "^$(HELM_REPO_NAME)$$"; then \
		echo "Adding $(HELM_REPO_NAME) Helm repo..."; \
		helm repo add $(HELM_REPO_NAME) $(HELM_REPO_URL) || true; \
	fi
	@echo "Updating Helm repos..."
	helm repo update

helm-fetch:
	@echo "Fetching fallback charts from GitHub into $(HELM_CHART_REPO_DIR) ..."
	@mkdir -p $(dir $(HELM_CHART_REPO_DIR))
	@if [ -d "$(HELM_CHART_REPO_DIR)/.git" ]; then \
		git -C "$(HELM_CHART_REPO_DIR)" fetch --all --tags && git -C "$(HELM_CHART_REPO_DIR)" checkout -qf $(HELM_CHART_REF) && git -C "$(HELM_CHART_REPO_DIR)" reset --hard; \
	else \
		git clone --depth 1 --branch $(HELM_CHART_REF) https://github.com/acryldata/datahub-helm.git "$(HELM_CHART_REPO_DIR)"; \
	fi

datahub-install: helm-repo
	@echo "Creating namespace $(NS) if not exists..."
	kubectl get ns $(NS) >/dev/null 2>&1 || kubectl create ns $(NS)
	@echo "Installing/upgrading prerequisites (Kafka, Zookeeper, Elasticsearch, DB)..."
	( helm upgrade --install $(RELEASE_PREREQ) $(HELM_REPO_CHART_PREREQ) -n $(NS) -f $(PREREQ_VALUES) ) || \
	( echo "Repo install failed; using local fallback chart" && $(MAKE) helm-fetch && \
	helm upgrade --install $(RELEASE_PREREQ) $(HELM_CHART_PREREQ_PATH) -n $(NS) -f $(PREREQ_VALUES) --dependency-update )
	@echo "Installing/upgrading DataHub..."
	( helm upgrade --install $(RELEASE_DATAHUB) $(HELM_REPO_CHART_DATAHUB) -n $(NS) -f $(DATAHUB_VALUES) ) || \
	( echo "Repo install failed; using local fallback chart" && $(MAKE) helm-fetch && \
	helm upgrade --install $(RELEASE_DATAHUB) $(HELM_CHART_DATAHUB_PATH) -n $(NS) -f $(DATAHUB_VALUES) --dependency-update )
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
	bash scripts/check_datahub.sh --namespace $(NS) --release $(RELEASE_DATAHUB)

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

# ---------- Postgres Testbed ----------

PG_SVC ?= postgres
PG_PORT_LOCAL ?= 15432

pg-up:
	@echo "Applying PG secrets..."
	kubectl -n $(NS) apply -f infra/k8s/pg-secrets.yaml
	@echo "Creating ConfigMaps for init scripts and seed..."
	kubectl -n $(NS) create configmap pg-init-scripts --from-file=sample/postgres/init.sh --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n $(NS) create configmap pg-seed --from-file=seed.sql=sample/postgres/seed.sql --dry-run=client -o yaml | kubectl apply -f -
	@echo "Applying Postgres Deployment/Service/PVC..."
	kubectl -n $(NS) apply -f infra/k8s/postgres.yaml
	@echo "Waiting for Postgres to be ready..."
	kubectl -n $(NS) rollout status deploy/postgres --timeout=180s
	@echo "Postgres is up. Service: $(PG_SVC)"

pg-load:
	@echo "Re-applying seed.sql into database..."
	POD=$$(kubectl -n $(NS) get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}'); \
	echo "Using pod $$POD"; \
	kubectl -n $(NS) exec -i $$POD -- sh -lc 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /seed/seed.sql'
	@echo "Seed loaded."

pg-ingest:
	@echo "Resolving GMS URL..."
	GMS_URL=$$(minikube -p minikube service -n $(NS) $(RELEASE_DATAHUB)-datahub-gms --url | head -n1); \
	echo "GMS: $$GMS_URL"; \
	T001_PASS=$$(kubectl -n $(NS) get secret pg-secrets -o jsonpath='{.data.T001_PASSWORD}' | base64 -d); \
	T002_PASS=$$(kubectl -n $(NS) get secret pg-secrets -o jsonpath='{.data.T002_PASSWORD}' | base64 -d); \
	( \
		set -e; \
		python3 -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r scripts/requirements.txt; \
		# Port-forward Postgres
		mkdir -p .portfw; \
		kubectl -n $(NS) port-forward svc/$(PG_SVC) $(PG_PORT_LOCAL):5432 > .portfw/pg.log 2>&1 & \
		PF_PID=$$!; \
		sleep 2; \
		# Ingest t001
		( . .venv/bin/activate; \
		  export DATAHUB_GMS=$$GMS_URL PG_HOST=127.0.0.1 PG_PORT=$(PG_PORT_LOCAL) PG_DB=sandbox PG_USER=t001 PG_PASSWORD="$$T001_PASS" PG_SCHEMA_PATTERN='t001'; \
		  datahub ingest -c infra/ingest/pg-recipe.yml ); \
		# Ingest t002
		( . .venv/bin/activate; \
		  export DATAHUB_GMS=$$GMS_URL PG_HOST=127.0.0.1 PG_PORT=$(PG_PORT_LOCAL) PG_DB=sandbox PG_USER=t002 PG_PASSWORD="$$T002_PASS" PG_SCHEMA_PATTERN='t002'; \
		  datahub ingest -c infra/ingest/pg-recipe.yml ); \
		# Stop port-forward
		kill $$PF_PID || true; \
	)
	@echo "Ingestion completed. Check DataHub UI for Postgres datasets."

pg-purge:
	@echo "Deleting Postgres resources..."
	- kubectl -n $(NS) delete -f infra/k8s/postgres.yaml --ignore-not-found
	- kubectl -n $(NS) delete secret pg-secrets --ignore-not-found
	- kubectl -n $(NS) delete configmap pg-init-scripts pg-seed --ignore-not-found
	@echo "Note: PVCs may remain (postgres-data). Delete manually if desired."

# Backward-compatible aliases for pg targets
pg\:up:
	@$(MAKE) pg-up
pg\:load:
	@$(MAKE) pg-load
pg\:ingest:
	@$(MAKE) pg-ingest
pg\:purge:
	@$(MAKE) pg-purge
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

classifier-run:
	@echo "Running regex-based PII classifier..."
	python3 -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r services/pii-classifier/requirements.txt
	. .venv/bin/activate && PYTHONPATH=services/pii-classifier python -m pii_classifier.cli $(CLASSIFIER_ARGS)

classifier\:run:
	@$(MAKE) classifier-run
