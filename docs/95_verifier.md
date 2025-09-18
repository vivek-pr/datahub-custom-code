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
   data, runs ingestion, and executes the regex classifier.
2. `make poc:verify` — executes `tools/verify_poc.py` which validates the full tokenization
   flow and writes JSON + JUnit artifacts.
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

## Verifier Details

`tools/verify_poc.py` orchestrates the assertions described in the issue:

1. Cluster readiness — nodes ready, critical workloads running.
2. DataHub health — GraphQL health check plus tag roundtrip.
3. Metadata — ensures the ingested Postgres dataset exists and PII tags are present.
4. Tokenization run — triggers tokenization and asserts `rowsAffected > 0`.
5. Idempotency probe — confirms repeated triggers do not mutate data.
6. Negative path — executes a dry-run request that must fail without data changes.
7. Observability — captures logs and verifies required log files.

Each step emits structured status in `report.json`. On failure, the verifier exits non-zero,
leaving the stack intact for debugging (unless you re-run `make poc:smoke`, which always
collects logs before teardown).

## Troubleshooting Matrix

| Symptom | Likely Cause | Fix |
| --- | --- | --- |
| `cluster-health` fails with `missing` workloads | Helm chart still rolling out or pod crashloop | Check `kubectl get pods -n datahub`, inspect pod logs under `artifacts/logs/`. |
| `datahub-readiness` reports unhealthy | GMS not reachable / misconfigured service | Verify `minikube service -n datahub datahub-datahub-gms --url` and confirm port-forward if running locally. |
| `ingested-metadata` missing PII tags | Classifier not run or ingestion incomplete | Re-run `make poc:up` to execute ingestion + classifier, ensure classifier pod logs mention tagged fields. |
| `tokenization` stuck in RUNNING | Tokenization worker not deployed or failing auth | Check `actions` worker logs, confirm database credentials/permissions. |
| `idempotency` reports additional writes | Token detection regex mismatch | Inspect `summarize_tokenization` logic in `tools/verify_poc.py` and adjust token format/pattern. |
| `negative-path` mutates data | DB permission drop not applied | Ensure failure path toggles permissions correctly; confirm run history in DataHub UI. |

## CI Integration

The workflow `.github/workflows/poc-smoke.yml` executes `make poc:smoke` for pull requests and
nightly builds. Artifacts are uploaded for post-mortem triage whenever the run fails.

## Cleaning Up

If you interrupted a run or want to manually stop the cluster:

```bash
make poc:destroy
```

Set `KEEP_CLUSTER=1` to skip the Minikube stop step when you plan to inspect the cluster manually.
