# POC Verifier (Minikube)

This document explains how to provision the proof-of-concept stack, run the end-to-end
verifier, inspect artifacts, and tear everything down. The goal is a single deterministic
entrypoint that stands up DataHub, Postgres, the classifier worker, and the tokenization
action before validating the full workflow end-to-end.

## Quickstart

```bash
# Provision → verify → tear down (destroys cluster unless KEEP_CLUSTER=1)
make poc:smoke
```

The orchestrator performs three phases:

1. `make poc:up` — starts Minikube, installs the Helm charts, deploys Postgres, seeds
   data, runs ingestion, executes the regex classifier, and deploys the tokenization
   actions worker.
2. `make poc:verify` — executes `tools/verify_poc.py` which validates the full tokenization
   flow (cluster health → Postgres → DataHub → tokenization runs → rollback checks) and
   writes JSON + JUnit artifacts.
3. `make poc:destroy` — tears everything down (unless `KEEP_CLUSTER=1`).

Artifacts are always written under `artifacts/`:

- `artifacts/verify/report.json` — machine readable step results.
- `artifacts/verify/junit.xml` — JUnit suite for CI visibility.
- `artifacts/logs/*.log` — logs collected from key pods.
- `artifacts/env/summary.txt` — tool versions for reproducibility.

## Individual Targets

| Target | Purpose |
| --- | --- |
| `make poc:up` | Provision the full stack. Safe to re-run; idempotent by design. |
| `make poc:verify` | Execute the verifier only (requires the stack to already be up). |
| `make poc:logs` | Collect logs from DataHub, classifier, actions, and Postgres pods. |
| `make poc:destroy` | Remove Helm releases, delete Postgres manifests, optionally stop Minikube. |
| `make poc:smoke` | Run `poc:up` → `poc:verify` with automatic teardown + log capture. |

### Flags & Environment

- `KEEP_CLUSTER=1 make poc:smoke` retains the Minikube cluster after the run so you can
  inspect resources manually. Logs are still captured.
- `POC_TIMEOUT` controls verifier polling duration (default `1200` seconds).
- `POC_TENANT`, `POC_DATASET_URN`, and `POC_REQUEST_ID` tweak verifier defaults.
- `DATAHUB_GMS` overrides the inferred GMS endpoint (otherwise resolved via `minikube service`).
- `HELM_USE_LOCAL_CHARTS=1 SKIP_HELM_FETCH=1 make …` forces the installer to use the vendored
  charts under `.helm-charts/` without reaching out to `helm.acryldata.io`. CI enables this mode
  automatically so smoke tests stay deterministic even when outbound DNS is restricted.

## Verifier Details

`tools/verify_poc.py` orchestrates the assertions described in the issue:

1. Cluster readiness — nodes ready, namespaces present, critical workloads running.
2. DataHub health — GraphQL health check plus tag roundtrip to confirm write/read path.
3. Postgres health — verifies tenant credentials (`SELECT 1`), proves cross-tenant
   access is denied, and captures a baseline snapshot of PII columns.
4. Metadata — ensures the ingested dataset exists, records PII-tagged schema fields,
   and caches the pre-tokenization row set for later comparisons.
5. Tokenization run — triggers the action, waits for DataHub run metadata, checks that
   `rowsAffected > 0`, validates the DataProcessInstance context (tenant, columns,
   external URL), and confirms the database values now match the deterministic token
   pattern.
6. Idempotency probe — replays the same `request_id`, asserts the run is successful but
   `rowsAffected == 0`, and verifies the database snapshot is unchanged.
7. Negative path — restores the dataset to the baseline values, temporarily revokes the
   tenant's `UPDATE` grant, triggers another run (expecting failure with zero writes),
   then re-enables permissions and re-applies tokenization so the system returns to a
   healthy state.
8. Observability — collects logs for DataHub + worker pods, confirms the actions log has
   `tenant_id` and `rows_affected`, verifies a request identifier is available (log or
   run context), and pings the metrics endpoint when it is exposed via environment
   configuration.

Each step emits structured status in `report.json`. On failure, the verifier exits non-zero,
leaving the stack intact for debugging (unless you re-run `make poc:smoke`, which always
collects logs before teardown).

## Troubleshooting Matrix

| Symptom | Likely Cause | Fix |
| --- | --- | --- |
| `cluster-health` fails with `missing` workloads | Helm chart still rolling out or pod crashloop | Check `kubectl get pods -n datahub`, inspect pod logs under `artifacts/logs/`. |
| `datahub-readiness` reports unhealthy | GMS not reachable / misconfigured service | Verify `minikube service -n datahub datahub-datahub-gms --url` and confirm port-forward if running locally. |
| `ingested-metadata` missing PII tags | Classifier not run or ingestion incomplete | Re-run `make poc:up` to execute ingestion + classifier, ensure classifier pod logs mention tagged fields. |
| `tokenization` stuck in RUNNING | Tokenization worker not deployed or failing auth | Check `actions` worker logs, confirm database credentials/permissions, and ensure `TOKENIZE_*` env vars reference valid secrets. |
| `idempotency` reports additional writes | Token detection regex mismatch or stale snapshot | Inspect `summarize_tokenization` logic in `tools/verify_poc.py`, confirm dataset values already tokenized before replay. |
| `negative-path` mutates data | DB permission drop not applied | Ensure the verifier can reach the Postgres pod and that `pg-secrets` contains the admin password so revokes succeed. |
| Observability step fails on metrics | Metrics port disabled or curl missing | Either set `TOKENIZE_METRICS_PORT` when deploying or install `curl`/`wget` into the container image. |

## CI Integration

The workflow `.github/workflows/poc-smoke.yml` executes `make poc:smoke` for pull requests and
nightly builds. Artifacts are uploaded for post-mortem triage whenever the run fails.

## Cleaning Up

If you interrupted a run or want to manually stop the cluster:

```bash
make poc:destroy
```

Set `KEEP_CLUSTER=1` to skip the Minikube stop step when you plan to inspect the cluster manually.
