# Tokenize POC for DataHub Actions

This repository contains a runnable proof-of-concept that implements the **"Inside DataHub" Tokenization (PG + Databricks)** design. It deploys a lightweight DataHub Action called `tokenize-poc` which can be manually triggered to route to Postgres or Databricks based on the dataset URN, tokenize PII columns via a thin SDK adapter, and write the results back using deterministic dummy tokens.

## Quick start

Prerequisites:

* [minikube](https://minikube.sigs.k8s.io/docs/)
* [kubectl](https://kubernetes.io/docs/tasks/tools/)
* [helm](https://helm.sh/)
* `envsubst` (GNU `gettext`)
* `python3` for running local tests

Clone the repo, copy the example secrets, and edit them with your connection details:

```bash
cp k8s/secrets.example.env k8s/secrets.env
$EDITOR k8s/secrets.env
```

Bring everything up, run the smoke job for both Postgres and Databricks (if configured), and tear it down:

```bash
make up
make run
make down
```

Additional helpers:

* `make logs` – stream the action deployment logs
* `make trigger-pg` / `make trigger-dbx` – manually invoke the action via Kubernetes curl pod
* `make test` – run unit and integration tests locally (spins up a disposable Postgres)

## Secrets and configuration

The action expects three secrets, rendered into `k8s/secrets.yaml` by `make up`.

| Key | Purpose |
| --- | --- |
| `PG_CONN_STR` | PostgreSQL connection string (user must have `SELECT`/`UPDATE`) |
| `DBX_JDBC_URL` | Databricks SQL Warehouse JDBC URL (only required to exercise the Databricks path) |
| `TOKEN_SDK_MODE` | Token SDK selector. `dummy` is the provided adapter. |

Edit `k8s/secrets.env` before running `make up`. The script base64-encodes the values and applies the secret manifest automatically.

## Architecture overview

![Context diagram placeholder – create docs/context.png showing DataHub Action talking to Postgres in-cluster and Databricks via JDBC](docs/context.png)

![Sequence diagram placeholder – create docs/sequence.png showing trigger → SELECT → SDK → UPDATE/MERGE → logs](docs/sequence.png)

![Deployment diagram placeholder – create docs/deployment.png showing minikube namespace with DataHub, Action, Postgres, and external Databricks](docs/deployment.png)

### Components

* **FastAPI action service** (`action/`): exposes `/healthz` and `/trigger` endpoints and orchestrates routing.
* **Token SDK adapter** (`action/sdk_adapter.py`): encapsulates the tokenization call site. Swapping in a real SDK later only requires replacing the adapter implementation.
* **Token logic** (`action/token_logic.py`): deterministic dummy token implementation `tok_<base64>_poc` and detector regex for idempotency.
* **Database handlers** (`action/db_pg.py`, `action/db_dbx.py`): execute SELECT + UPDATE or MERGE loops with explicit detection to skip already-tokenized values.
* **Kubernetes manifests** (`k8s/`): namespace, secrets template, Bitnami Postgres values, action deployment/service, RBAC, and a smoke-test Job.
* **Operational scripts** (`scripts/`): one-command bring-up/teardown, Postgres seeding, health waits, and CLI trigger helper.

## How it works

1. `make up` starts/uses the `tokenize-poc` minikube profile, installs Bitnami Postgres, loads the FastAPI action container, applies manifests, seeds 100 dummy customer rows, and verifies `/healthz`.
2. Triggering the action (via `scripts/trigger.sh`, the smoke Job, or DataHub) sends the dataset URN and column list.
3. The action parses the URN to determine the platform (`postgres` or `databricks`).
4. The adapter lazily instantiates the SDK (`TOKEN_SDK_MODE=dummy`) and returns deterministic tokens (`tok_<base64>_poc`).
5. Postgres path: a single transaction performs `SELECT ... FOR UPDATE` with a regex guard, tokenizes in-memory, and issues targeted `UPDATE`s. Commit occurs only when all updates succeed.
6. Databricks path: uses the SQL Warehouse HTTP connector derived from the JDBC URL to `SELECT` and `UPDATE` (or effectively merge) the few matching rows.
7. Each run logs updated vs skipped counts and elapsed time, and returns them in the HTTP response.
8. Re-running with the same input results in zero updates because the detector skips existing tokens.

## Why this satisfies the POC goals

* **Tokenization from a DataHub Action** – The FastAPI service is intended to be packaged as an Action container; the provided deployment manifests and smoke job demonstrate invoking it just like DataHub would.
* **Connection strings only** – The action reads `PG_CONN_STR` and `DBX_JDBC_URL` from Kubernetes secrets and never shells out or requires extra credentials. Tests confirm Postgres round-trips; Databricks code paths are exercised by unit tests and optionally integration-tested when credentials are present.
* **Deterministic, idempotent tokens** – `tok_<base64>_poc` is both deterministic and easily detectable via regex, ensuring repeat runs are no-ops.
* **Routing and rollback** – Platform detection uses the dataset URN, and the Postgres handler wraps the update loop in a single transaction. Databricks relies on Warehouse semantics (autocommit per statement) so a failing update aborts without a partial commit.
* **Swap-in readiness** – The adapter isolates the actual tokenization call; switching to a real SDK later does not touch the HTTP or database logic.

## Testing strategy

* **Unit tests** (`tests/test_token_logic.py`, `tests/test_dbx_integration.py`) verify determinism and regex detection, and ensure the Databricks JDBC parser handles the documented format.
* **Postgres integration test** (`tests/test_pg_integration.py`) spins up a disposable local Postgres (using the system binaries), seeds 100 rows, ensures the first run updates them, and confirms the second run produces zero updates.
* **Databricks integration test** is marked `skip` unless `DBX_TEST_JDBC_URL` is set, preventing failures when a warehouse is unavailable.
* **Smoke Job** (`k8s/smoke-job.yaml`) performs two back-to-back triggers for each platform and fails the Job if the second run updates anything.

Run the automated test suite locally with:

```bash
make test
```

This command creates a Python virtual environment, installs tooling (`ruff`, `black`, `pytest`), lints the code, and then executes all tests. The Postgres server is temporary and bound to `127.0.0.1:55432`.

## Implementation details

* **Dataset URN parsing** – `action/models.py` extracts the platform and `database.schema.table` triplet. For Databricks, it renders fully-qualified object names `` `catalog`.`schema`.`table` `` so multi-catalog warehouses work out of the box.
* **SQL guards** – Both database integrations wrap the `SELECT` with regex guards (`!~` for Postgres, `NOT RLIKE` for Databricks) so only plain-text rows are fetched. Updates only touch columns that changed, preserving untouched values.
* **Logging** – All major steps log counts and timings, making the smoke Job and `make logs` output easy to inspect.
* **Security context** – The container runs as non-root, and the Bitnami Postgres deployment inherits hardened security contexts.
* **Idempotent scripts** – `make up` is safe to re-run; Helm upgrades are idempotent, secrets are re-applied, and the seed script only populates data when the table is empty.

## Risks and limitations

* **Databricks JDBC latency** – HTTP-based SQL warehouses are slower than Spark-native pipelines, but this is acceptable for tiny POC batches.
* **Dummy token** – The adapter intentionally uses a reversible format for the POC. Swapping in a production SDK only requires updating `TokenizationSDKAdapter`.
* **Minimal observability** – No lineage, audit, or alerting hooks are implemented. This keeps the footprint small and focused on the happy path.
* **Single-namespace scope** – High availability, multi-tenant isolation, and security hardening are out of scope for the POC.

## Troubleshooting

* `make up` fails with `secrets.env missing` – copy `k8s/secrets.example.env` to `k8s/secrets.env` and populate it before retrying.
* `minikube` cannot start with the Docker driver – remove the `--driver=docker` flag from `scripts/minikube_up.sh` or choose a different driver.
* Smoke Job fails on the Databricks step – ensure the SQL Warehouse has an `id` primary key column and that `DBX_JDBC_URL` points to an active warehouse with write permissions.
* Local tests complain about missing Postgres binaries – install the `postgresql` package (or adjust `PATH` so `initdb` and `pg_ctl` are available).
* Action pod stuck in CrashLoop – run `make logs` to inspect FastAPI logs; misconfigured connection strings or missing secrets are the usual culprits.

## Clean teardown

`make down` deletes the namespace and the minikube profile, guaranteeing there are no lingering resources or credentials once you are done experimenting with the POC.
