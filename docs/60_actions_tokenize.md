Tokenize Action

- Why: POC the in-DataHub orchestration path—adding the `tokenize-now` tag in the UI should route through the Actions framework, run Postgres updates in one transaction per table, and surface run status directly on the dataset.
- What: Python action plugin (`actions_tokenize.TokenizeAction`) packaged into `services/actions-tokenize`. It listens to global tag mutations, filters on `urn:li:tag:tokenize-now`, intersects with existing `pii-*` field tags, and executes deterministic tokenization statements through tenant-scoped Postgres credentials.

Enablement

- Build image + deploy: `make actions:up` (builds `datahub-actions-tokenize:latest` and applies `infra/k8s/actions-tokenize.yaml`).
- Tear down: `make actions:down` removes the worker deployment.
- Tail logs: `make actions:logs` streams container output; JSON lines expose `event` keys for filters.
- Image only: `make actions:image` builds the Docker image without touching the cluster.

Config & Secrets

- Config file baked into the image: `/opt/actions-tokenize/config/actions-tokenize.yaml`. It delegates to env vars for runtime overrides (e.g., `TOKENIZE_PG_HOST`, `TOKENIZE_T001_PASSWORD`, `TOKENIZE_DRY_RUN`).
- Postgres credentials come from `pg-secrets` (`T001_PASSWORD`, `T002_PASSWORD`). Each tenant entry controls schema/role overrides and keeps privileges scoped.
- Kafka + Schema Registry wiring inherits cluster defaults via `KAFKA_BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL`. Adjust for external brokers if running outside the Helm stack.
- DataHub API access relies on `DATAHUB_GMS_URL` (auth disabled in the POC chart; set `DATAHUB_GMS_TOKEN` when pointing at a secured deployment).

Triggering tokenization

1. Confirm pii tags exist on the target fields (classifier or manual tagging).
2. In the DataHub UI, add tag `urn:li:tag:tokenize-now` at the dataset level (bulk) or on a specific schema field (granular). No custom UI needed.
3. The action resolves the dataset URN, grabs PII-tagged columns, and emits a DataProcessInstance run: `RUNNING` → `COMPLETE` (`SUCCESS` or `FAILURE`).
4. On success the Postgres rows update to `tok_<hash/base64>_poc` format; re-tagging the dataset is idempotent (zero rows affected when already tokenized).

Observability & Metadata

- Structured logs: each major step logs a JSON object (`event=run.started`, `event=tokenize.success`, etc.) with dataset URN, tenant, and counts.
- Run metadata: `Tokenize Action Flow` / `Tokenize Action Job` show up under the dataset’s lineage → Runs tab. Runtime context includes total + per-column rows, tenant id, dry-run flag, and any errors.
- External URLs: configure `TOKENIZE_EXTERNAL_URL_BASE` (e.g., DataHub frontend) to surface deep links from run cards.

Safety & Failure Modes

- Transactions: one `UPDATE ... WHERE` per table, wrapped in a transaction. The executor rolls back on any exception; no partial commits.
- Regex guard: `regex_token_pattern` rejects already-tokenized values so replays are no-ops.
- Dry run: set `TOKENIZE_DRY_RUN=true` to exercise end-to-end metadata without touching Postgres (rowsAffected stays `0`).
- Column cap: `TokenizeActionConfig.max_columns` can bound per-run blast radius; hitting the limit raises an error and the run is marked `FAILURE`.
- Negative paths: missing DB perms, network failures, or schema drift trigger a failure run event with the exception message in `run_result`.

Disable / iterate

- Disable worker: `make actions:down`.
- Update config: tweak env vars in `infra/k8s/actions-tokenize.yaml` (or patch the deployment) and redeploy.
- Extend coverage: add tenant entries in the config YAML, tune custom properties surfaced on the run entity, or wire dashboards to the JSON logs.

Verification checklist

- Dataset tagged with `tokenize-now` shows a new run: `RUNNING` → `COMPLETE (SUCCESS)` with non-zero `rowsAffected`.
- Postgres table values match `tok_<...>_poc` for tagged columns; rerun produces `rowsAffected = 0`.
- `make actions:logs` reveals `tokenize.success` for successful runs and `tokenize.error` when credentials or permissions fail.
