DataHub on Minikube (Helm)

- Why: Stand up a local DataHub you can automate against.
- What: Helm-based deploy on Minikube with lightweight values, Makefile targets, and health checks.

Prereqs

- kubectl, helm, minikube, bash, Python 3.9+
- Enough resources: at least 4 CPUs and 8 GB RAM dedicated to Minikube

Quick Start

1) Start Minikube

- `make mk:up`

2) Install DataHub (and prerequisites)

- `make datahub:install`

3) Check status and health

- `make datahub:status`

4) Optionally port-forward for local access

- `make datahub:portfw`
- UI: `http://localhost:9002`
- GraphiQL: `http://localhost:8080/api/graphiql`

5) Run integration / smoke tests (optional)

- Create venv and install SDKs automatically and run checks:
  - Integration (GraphQL + REST emitter tag): `make datahub:test:integration`
  - E2E smoke (ingest sample dataset): `make datahub:test:e2e`

Helm Configuration

- DataHub values: `infra/helm/datahub-values.yaml` (ClusterIP services, reduced resources)
- Prerequisites values: `infra/helm/prerequisites-values.yaml` (Single-node Kafka & Elasticsearch)

Make Targets

- `mk:up`: Start Minikube with sane defaults (CPUs, RAM). Override with `MK_CPUS`, `MK_MEMORY`, `MK_DISK`.
- `datahub:install`: Add Helm repo, install `acryldata/prerequisites` then `acryldata/datahub` using the values overrides.
- `datahub:status`: Show pods/services and run GraphQL health checks via `scripts/check_datahub.sh`.
- `datahub:portfw`: Port-forward GMS (8080) and Frontend (9002) locally.
- `datahub:uninstall`: Uninstall DataHub and prerequisites releases.

Health Checks

- Script: `scripts/check_datahub.sh`
  - Resolves GMS URL via `minikube service` (or `--gms-url`)
  - Probes `GET /api/graphiql` (200)
  - Probes `POST /api/graphql` with introspection query (200)
  - Pings REST path `/entities` (accepts 2xx/405)

Troubleshooting

- Helm repo add fails or DNS resolution issues
  - Retry `helm repo add acryldata https://helm.acryldata.io && helm repo update`
  - Check network/VPN and that `curl https://helm.acryldata.io/index.yaml` resolves

- Pods stuck Pending
  - Increase resources: `make MK_CPUS=6 MK_MEMORY=12288 mk:up`
  - Ensure a default StorageClass exists for ES/MySQL PVs: `kubectl get sc`

- Pods crashlooping
  - Check logs: `kubectl -n datahub logs deploy/datahub-datahub-gms -f`
  - Verify Kafka/ES are healthy: `kubectl -n datahub get pods | grep -E 'elasticsearch|kafka|zookeeper'`

- Cannot access UI/GraphQL
  - Use `make datahub:portfw` and browse `http://localhost:9002` and `http://localhost:8080/api/graphiql`
  - Or use `minikube service -n datahub datahub-datahub-frontend --url`

Cleanup

- `make datahub:uninstall`
- PVs may remain (ES/MySQL). To fully clean: `kubectl -n datahub delete pvc --all` and delete lingering PVs if desired.

