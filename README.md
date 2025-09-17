DataHub on Minikube via Helm + Makefile

- Goal: Stand up a local DataHub you can automate against.

Quickstart

- Start cluster: `make mk-up` (alias: `make mk:up`)
- Install DataHub: `make datahub-install` (alias: `make datahub:install`)
- Check health: `make datahub-status` (alias: `make datahub:status`)
- Port-forward (optional): `make datahub-portfw` (alias: `make datahub:portfw`) then open UI at `http://localhost:9002` and GraphiQL at `http://localhost:8080/api/graphiql`

Docs

- Full setup & troubleshooting: `docs/20_datahub.md`

What’s included

- `infra/helm/datahub-values.yaml`: Lightweight overrides for Minikube (ClusterIP, reduced resources)
- `infra/helm/prerequisites-values.yaml`: Single-node Kafka & Elasticsearch
- `scripts/check_datahub.sh`: Health checker for GMS GraphQL and REST liveness
- `scripts/test_datahub_integration.py`: Verifies GraphQL 200 and tag upsert via REST emitter
- `scripts/test_datahub_e2e.py`: Ingests a sample dataset and verifies via GraphQL
- Postgres testbed:
  - `infra/k8s/postgres.yaml`, `infra/k8s/pg-secrets.yaml`
  - `sample/postgres/init.sh`, `sample/postgres/seed.sql`
  - `infra/ingest/pg-recipe.yml`

Notes

- Helm repo: `acryldata` (`https://helm.acryldata.io`). If unreachable (DNS/VPN), the Makefile automatically falls back to cloning `acryldata/datahub-helm` and installing from local chart paths. Requires `git`.
- You can pin the fallback chart ref via `HELM_CHART_REF` (default `master`).
- Make targets use `helm upgrade --install` with the values files

Postgres Testbed

- Bring up Postgres: `make pg-up`
- Reload seed data: `make pg-load`
- Ingest into DataHub: `make pg-ingest`
- Purge resources: `make pg-purge`

- Tests require Python and the DataHub SDK; install via `make datahub:test:integration` or `make datahub:test:e2e`
