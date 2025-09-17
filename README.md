POC for orchestrating tokenization of PII columns with DataHub as system-of-record. Triggers via DataHub Actions, runs per-tenant with least-privilege creds, in-place safe writes, full run/audit recorded in DataHub. Includes: Minikube deployment, DataHub Helm, Postgres sample data, regex PII classifier, Postgres runner, Databricks integration, docs, unit & integration tests, end-to-end scripts.

Governance

- docs: see `docs/00_overview.md` (scope/constraints) and `docs/decisions.md` (ADR-0001).
- linting: pre-commit hooks configured in `.pre-commit-config.yaml`.
- CI: GitHub Actions runs pre-commit and a smoke check on push/PR.
- local usage: `pipx install pre-commit && pre-commit install` then commit as usual; or run `pre-commit run -a`.

Repo skeleton (at a glance)

docs/ (setup, design, runbook, API, test plans)

make/ (reusable scripts)

infra/

k8s/ (namespaces, RBAC, secrets templates, services, jobs)

helm/ (DataHub chart values overrides)

services/

pii-classifier/ (regex + column-name heuristics → emits tags/aspects)

actions-tokenize/ (DataHub Action: watch trigger → open run → dispatch work → close run)

pg-runner/ (REST + Job mode: connects to PG, applies SDK, transactional update)

dbx-runner/ (optional early stub; later: Databricks Jobs API client)

sdk-adapter/ (thin adapter over your in-house Tokenization SDK; mockable)

cli/ (emit trigger, check status)

tests/ (unit/integration/e2e)

sample/

postgres/seed.sql (customers, payments with PII)

regex/ (rules.yml)

Makefile (key targets)

make mk:up|down (minikube start/stop with resources)

make datahub:install|uninstall (helm install DataHub, minimal profile)

make pg:up|load|purge (PG Deployment, Service, seed data)

make build (all images) / make push (load into Minikube)

make deploy (pii-classifier, actions-tokenize, pg-runner, secrets)

make trigger (CLI emit custom aspect/tag with tenant_id/request_id)

make status (CLI fetch DataHub run status)

make test:unit|int|e2e:pg|e2e:dbx

make logs SERVICE=actions-tokenize / make clean
