# DataHub Tokenization POC – End-to-End Flow

This proof of concept wires the "Inside DataHub Tokenization (PG + Databricks)" action into a
full UI-triggered experience. Adding the tag `tokenize/run` to a dataset from the DataHub UI
causes the action to discover PII columns, tokenize them in the source system, and write a status
summary back to the dataset page.

```
DataHub UI ──┐
             ▼
      tokenize/run tag               ┌─────────────┐
             │                       │  DataHub    │
             │                       │  Action     │
             │                       └────┬────────┘
             ▼                            │
   DataHub Metadata ⟵───────┐             │
             │              │             ▼
             │        PII detector     Postgres /
             │              │           Databricks
             │              │             │
             ▼              │             ▼
 last_tokenization_run ◀────┴───── tokenized values + tags
```

## Quick start

```bash
make build && make up              # build the Docker image and deploy the action
make ingest                        # register datasets via DataHub ingestion
make seed-pg                       # load demo customers(id,email,phone)
make trigger-ui                    # add tokenize/run to the sample dataset
make wait-status                   # block until last_tokenization_run.status == SUCCESS
make verify-idempotent             # re-run to ensure zero rows updated
make down                          # tear everything down when finished
```

`make e2e` runs the entire happy path: `build → up → ingest → seed-pg → trigger-ui → wait-status → verify-idempotent`.

## Prerequisites

* Docker, kubectl, and helm on your PATH
* minikube (default) or kind (`CLUSTER=kind`)
* `jq` for the helper scripts
* A running DataHub instance with access to `DATAHUB_GMS` and (optionally) `DATAHUB_TOKEN`

The first `make up` copies `k8s/secrets.example.env` to `k8s/secrets.env`. Fill it in with the
connection strings for:

* `PG_CONN_STR` – the Postgres instance to tokenize
* `DATAHUB_GMS`/`DATAHUB_TOKEN` – REST endpoint + personal access token
* `DBX_JDBC_URL` – optional JDBC URL for a Databricks SQL warehouse (leave blank to skip)
* `DBX_CATALOG`, `DBX_SCHEMA`, `DBX_TABLE` – optional Databricks identifiers

These values are mounted into the action container and also drive the ingestion jobs.

## Ingestion recipes

`make ingest` runs two recipes using the `datahub` CLI (installed into `.venv/`):

* `ingestion/postgres.yml` – connects to the seeded Postgres instance (via a temporary port-forward)
  and registers the `public.customers` table.
* `ingestion/databricks.yml` – optional Databricks ingestion. The target is skipped cleanly when
  `DBX_JDBC_URL` is unset.

The CLI uses `DATAHUB_GMS`/`DATAHUB_TOKEN` for the REST sink. Sample dataset name: `public.customers`.
A corresponding dataset URN will look like:

```
urn:li:dataset:(urn:li:dataPlatform:postgres,<database>.public.customers,PROD)
```

`scripts/find_dataset_urn.py` will locate the exact URN by name + platform if you do not know it yet.

## Triggering from the UI (or CLI)

1. Navigate to the dataset in DataHub and add the tag `tokenize/run` at either the dataset or field level.
   The action polls the metadata change log and starts a run as soon as the tag appears.
2. The run discovers PII columns via `action/pii_detector.py`:
   * columns already tagged with `pii.*` or `sensitive`
   * otherwise heuristics for names like `email`, `phone`, `contact`, `ssn`, `aadhaar`
3. Each plaintext value becomes `tok_<base64>_poc` (idempotent; existing tokens are skipped).
4. Status is pushed back to DataHub in the editable properties under `last_tokenization_run` along with
   dataset tags `tokenize/done` and `tokenize/status:SUCCESS`.

The helper scripts make it easy to simulate UI actions:

```bash
scripts/add_tag.sh "$DATASET_URN"         # attach tokenize/run
scripts/poll_status.sh "$DATASET_URN"     # wait for SUCCESS and print the JSON status
scripts/e2e.sh                              # run once and then verify idempotency
```

A successful status payload looks like:

```json
{
  "run_id": "...",
  "started_at": "2024-06-01T12:00:00Z",
  "ended_at": "2024-06-01T12:00:04Z",
  "platform": "postgres",
  "columns": ["email", "phone"],
  "rows_updated": 100,
  "rows_skipped": 0,
  "status": "SUCCESS",
  "message": "Tokenized columns email, phone; updated 100 rows, skipped 0."
}
```

Re-running `scripts/add_tag.sh` after a successful run reports `rows_updated=0` and leaves `tokenize/run`
cleared from the dataset and fields.

## Action internals

* `action/mcl_consumer.py` polls DataHub via GraphQL for datasets or fields tagged with `tokenize/run`.
* `action/datahub_client.py` implements the GraphQL helpers for reading schema metadata, adding/removing
  tags, and writing editable dataset properties.
* `action/run_manager.py` orchestrates each run, including platform selection, tokenization, status
  emission, and tag hygiene (`tokenize/done`, `tokenize/status:*`, `tokenized`).
* `action/pii_detector.py` is pluggable: drop a YAML file and set `PII_CONFIG_PATH` to customise patterns.
* `action/db_pg.py` / `action/db_dbx.py` issue transactional updates with regex guards to keep tokens
  idempotent (`^tok_[A-Za-z0-9+/=]+_poc$`).

## Tests

```
make test        # ruff + black --check + shell printf validation + pytest
```

Unit tests cover the PII detector heuristics, token regex, and the status payload schema.

## Troubleshooting

* `make ingest` fails – ensure `datahub` CLI is installed via `make test` or `make ingest` (it lives in `.venv`).
* `scripts/poll_status.sh` keeps waiting – confirm the action pod logs (`kubectl -n tokenize-poc logs deploy/tokenize-poc-action`).
* No datasets discovered – run `python scripts/find_dataset_urn.py public.customers --platform postgres` to confirm
  ingestion registered the dataset and that the name matches `DATASET_NAME`/`DATASET_PLATFORM`.
* `tokenize/run` never clears – a failure keeps the tag so you can retry. Check `last_tokenization_run.status`
  for the error message and fix the underlying connectivity issue.
* Databricks optional – leave `DBX_JDBC_URL` blank to skip ingestion and tokenization for Databricks.

## Cleanup

```
make down
```

`make down` uninstalls the Helm release, deletes the Kubernetes namespace, and tears down the minikube/kind cluster
profile so reruns start cleanly.
