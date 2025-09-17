Postgres Testbed + DataHub Ingestion

- Why: Provide a multi-tenant sandbox and metadata in DataHub to drive classification & tokenization.

What’s Included

- `infra/k8s/postgres.yaml`: Deployment, Service, PVC for `postgres:15-alpine`.
- `infra/k8s/pg-secrets.yaml`: Secrets for superuser and two tenants: `t001`, `t002`.
- `sample/postgres/init.sh`: Init script to create tenant roles using secret passwords, runs on first boot.
- `sample/postgres/seed.sql`: Creates schemas `t001`, `t002`; tables `customers`, `payments`; inserts sample data; sets grants.
- `infra/ingest/pg-recipe.yml`: DataHub ingestion recipe parameterized via env.

Quickstart

- Start/Install DataHub: `make mk-up && make datahub-install`
- Bring up Postgres: `make pg-up`
- Load/Reload sample data: `make pg-load`
- Ingest to DataHub: `make pg-ingest`

Service & Credentials

- Namespace: `datahub` (default, configurable via `NS`)
- Service: `postgres:5432`
- Database: `sandbox`
- Roles:
  - `t001` / from secret `T001_PASSWORD`
  - `t002` / from secret `T002_PASSWORD`
  - Superuser `postgres` / from secret `POSTGRES_PASSWORD` (not used by ingestion)

ERD (per-tenant schema)

- Schema `t001` and `t002` each contain:
  - `customers(id, full_name, email, phone, pan, created_at)`
  - `payments(id, customer_id -> customers.id, amount, currency, status, created_at)`

Least-Privilege Model

- `t001` has USAGE on `schema t001` and DML on its tables/sequences only.
- `t002` has the same for `schema t002`.
- No cross-schema grants; `PUBLIC` access revoked.

Ingestion to DataHub

- Recipe: `infra/ingest/pg-recipe.yml` (uses env vars for connection and schema filtering)
- Make target `pg-ingest`:
  - Resolves DataHub GMS via `minikube service ... --url`
  - Port-forwards Postgres to `127.0.0.1:15432`
  - Runs two ingestions using the DataHub CLI:
    - `t001` with `schema_pattern: t001.*`
    - `t002` with `schema_pattern: t002.*`

Troubleshooting

- Postgres not ready: `kubectl -n datahub logs deploy/postgres -f`
- Reset data: `make pg-load` to re-apply `seed.sql`
- Ingestion fails:
  - Ensure DataHub GMS is running: `make datahub-status`
  - Check CLI logs in your terminal; try `PG_SCHEMA_PATTERN='t001.*'` or `'t002.*'` manually.

Cleanup

- `make pg-purge` (PVC may remain). To fully clean: `kubectl -n datahub delete pvc postgres-data`.

