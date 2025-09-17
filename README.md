DataHub on Minikube via Helm + Makefile

- Goal: Stand up a local DataHub you can automate against.

Quickstart

- Start cluster: `make mk:up`
- Install DataHub: `make datahub:install`
- Check health: `make datahub:status`
- Port-forward (optional): `make datahub:portfw` then open UI at `http://localhost:9002` and GraphiQL at `http://localhost:8080/api/graphiql`

Docs

- Full setup & troubleshooting: `docs/20_datahub.md`

What’s included

- `infra/helm/datahub-values.yaml`: Lightweight overrides for Minikube (ClusterIP, reduced resources)
- `infra/helm/prerequisites-values.yaml`: Single-node Kafka & Elasticsearch
- `scripts/check_datahub.sh`: Health checker for GMS GraphQL and REST liveness
- `scripts/test_datahub_integration.py`: Verifies GraphQL 200 and tag upsert via REST emitter
- `scripts/test_datahub_e2e.py`: Ingests a sample dataset and verifies via GraphQL

Notes

- Helm repo: `acryldata` (`https://helm.acryldata.io`)
- Make targets use `helm upgrade --install` with the values files
- Tests require Python and the DataHub SDK; install via `make datahub:test:integration` or `make datahub:test:e2e`

