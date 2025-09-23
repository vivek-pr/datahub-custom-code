# Tokenize POC for DataHub Actions

This repository packages the "Inside DataHub Tokenization (PG + Databricks)" proof of concept as a self-contained, reproducible environment. It builds a Dockerised FastAPI DataHub Action, provisions Postgres inside Kubernetes, and runs end-to-end smoke tests that prove deterministic tokenisation and idempotency for both the Postgres and optional Databricks paths.

## Prerequisites

Install the following tools locally:

* [Docker](https://docs.docker.com/get-docker/)
* [kubectl](https://kubernetes.io/docs/tasks/tools/)
* [helm](https://helm.sh/docs/intro/install/)
* Either [minikube](https://minikube.sigs.k8s.io/docs/) (default) or [kind](https://kind.sigs.k8s.io/) – set `CLUSTER=kind` for kind
* Python 3.11+

All Kubernetes manifests live in the `tokenize-poc` namespace by default. Secrets are rendered from `k8s/secrets.env`; the first run copies `k8s/secrets.example.env` and continues with the baked-in Postgres defaults. `DBX_JDBC_URL` is blank by default so the Databricks flow is skipped until you fill it in.

## One-liner workflow

```bash
make build && make up && make run && make down
```

* `make build` – builds the action Docker image (`tokenize-poc/action:local`).
* `make up` – starts minikube/kind, installs Bitnami Postgres, renders secrets, deploys the action, waits for `/healthz`, and seeds 100 demo rows.
* `make run` – port-forwards to the action and triggers the Postgres path twice (first run updates rows, second run is idempotent). If `DBX_JDBC_URL` is populated, the Databricks flow is executed with the same assertions.
* `make down` – removes the namespace, Helm release, and deletes the cluster/profile to leave no residue.

### Switching to kind locally

The Makefile auto-detects the cluster via the `CLUSTER` variable. To mirror the CI path locally, run:

```bash
make build CLUSTER=kind IMAGE_TAG=local
make up CLUSTER=kind
make run CLUSTER=kind
make down CLUSTER=kind
```

## Continuous integration

`.github/workflows/ci.yaml` runs on pushes and pull requests:

1. **lint-unit** installs dev requirements, runs ShellCheck across every script, and then executes `make test` (which covers `ruff`, `black --check`, `scripts/verify_printf.sh`, and `pytest`).
2. **e2e-kind** provisions a kind cluster, smoke-tests the lifecycle with `make up` / `make down`, rebuilds the cluster for the full `make up` / `make run` flow, and always tears resources down. On any failure, `scripts/diag.sh` captures pods, events, and logs which are uploaded as build artifacts.

## Toolbox

* `make test` – sets up a Python virtualenv, lints (`ruff`/`black`), asserts safe shell printing via `scripts/verify_printf.sh`, and runs all tests. `tests/test_pg_integration.py` spins up a disposable local Postgres to verify transactional behaviour.
* `make trigger-pg` / `make trigger-dbx` – manual invocations against the Kubernetes service.
* `make diag` – prints nodes, namespace resources, events, and recent action/Postgres logs.
* `make reset-pg` – sanitises stale Helm releases/resources in the namespace so the Bitnami chart can be re-installed cleanly.
* `make ci` – convenience target that runs the kind-based E2E locally.

`scripts/run_e2e.py` performs readiness checks by port-forwarding to the action and asserting the first trigger updates rows while the second updates none. `scripts/diag.sh` is also invoked automatically by CI whenever a failure occurs.

### Helm ownership mismatch auto-fix

If previous runs left Helm-managed resources (for example `NetworkPolicy/postgresql`) annotated for an older release, Helm 3 refuses to adopt them. `make up` invokes `scripts/helm_sanitize_pg.sh` to uninstall the stale release (if present) and delete conflicting resources before reinstalling `tokenize-poc-postgresql`. Run `make reset-pg` manually to perform the cleanup on demand.

## Implementation highlights

* **Deterministic tokens** – `tok_<base64(value)>_poc` lives in `action/token_logic.py`. Existing tokens are recognised via regex so re-processing is a no-op.
* **Idempotent Postgres updates** – `action/db_pg.py` executes `SELECT ... FOR UPDATE` in a single transaction and only issues targeted `UPDATE`s when plaintext values exist. Batch size is capped via the trigger `limit`.
* **Optional Databricks support** – `action/db_dbx.py` parses a Databricks JDBC URL, connects via the SQL connector, and applies the same tokenisation/detection loop. When `DBX_JDBC_URL` is absent, both the smoke test and API return a clear skip message.
* **Robust orchestration** – `scripts/up.sh` sanitises stray Helm ownership metadata before installing the Bitnami Postgres release, waits for `pg_isready`, probes `/healthz` from inside the cluster, and seeds Postgres idempotently. `scripts/down.sh` uninstalls the release, deletes conflicting resources, and tears the cluster down.
* **Non-root container** – the Docker image installs dependencies on `python:3.11-slim`, copies the app into `/app`, and switches to UID `10001` before launching `uvicorn`.

## Troubleshooting

* `make up` fails because dependencies are missing – ensure `docker`, `kubectl`, `helm`, and your chosen cluster driver (`minikube` or `kind`) are installed and visible in `$PATH`.
* Postgres pods stay Pending – check container runtime resources. `scripts/diag.sh` summarises pod status and events.
* `make run` hangs – confirm port 18080 is free and that the action pod reached `Ready`. `make logs` tails the deployment.
* `/healthz` probe fails – ensure the container command points to `action.app:app` in `docker/action.Dockerfile`, confirm the service and deployment share the `app.kubernetes.io/name=tokenize-poc-action` label, and verify endpoints exist via `kubectl -n tokenize-poc get endpoints tokenize-poc-action -o yaml`. Inspect pod logs with `kubectl -n tokenize-poc logs deploy/tokenize-poc-action` and reapply `k8s/networkpolicy-allow-action.yaml` if your cluster enforces default-deny.
* Databricks flow skipped – populate `DBX_JDBC_URL` inside `k8s/secrets.env`. The value is base64-encoded into a Kubernetes secret automatically.
* Helm refuses to install Postgres due to ownership mismatch – earlier runs may have left resources annotated for a different release. `make up` calls `scripts/helm_sanitize_pg.sh` automatically, or run `make reset-pg` manually to uninstall the stale release and delete conflicting resources before re-deploying.
* Want to re-run everything fresh – `make down` deletes the namespace and underlying cluster, making reruns idempotent.
* Saw `printf: '(' invalid format character` in older scripts – the logging utilities now render timestamps with `date` and quote all values before calling `printf`. Running `make test` executes `scripts/verify_printf.sh`, which guards against regressions involving percent signs or parentheses in data.

## Why this satisfies the HLD

The implementation demonstrates:

* **Deterministic, reversible dummy tokenisation** with explicit detection preventing re-tokenisation.
* **Transactional Postgres writes** using `SELECT ... FOR UPDATE` followed by batched `UPDATE`s inside a single commit.
* **Databricks parity** through the SQL Warehouse connector with opt-in testing guarded by secrets.
* **Operational safety** via health probes, readiness checks, and automated diagnostics for both local runs and CI.
* **Developer ergonomics** with `make` targets, reproducible Docker builds, and a single-command E2E that matches the CI pipeline.
