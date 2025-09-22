# DataHub + Postgres PII Encoding PoC

## Overview
This proof of concept runs the DataHub quickstart stack next to a demo Postgres database. When a Postgres ingestion finishes, a
regex-based PII classifier scans the ingested tables, hands the flagged columns to the Base64 encoder action, and writes results
to `encoded.<table>`. Only columns matched by the classifier are encoded; non-sensitive columns are copied as-is. Everything is
optimized for local experimentation—no production hardening or durability guarantees beyond the Docker containers it starts.

## Quick Start
1. **Launch the stack**
   ```bash
   make up
   ```
2. **Ingest Postgres metadata on demand**
   ```bash
   docker compose run --rm ingestion
   ```
3. **Run classifier + encoder manually (optional)**
   ```bash
   POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
   python -m scripts.run_classifier_and_encode \
     --pipeline-name postgres_local_poc \
     --platform postgres \
     --schema-allow public.*
   ```
4. **Inspect the encoded copy**
   ```bash
   docker compose exec postgres psql -U datahub -d postgres \
     -c "\dt encoded.*" \
     -c "SELECT email, phone_number FROM encoded.customers LIMIT 5;"
   ```

The UI-driven helper (`ui_ingestion_runner`) still polls DataHub for ingestion completions and now invokes the classifier
automatically. Logs for both the classifier and encoder appear under `docker compose logs -f ui-ingestion-runner base64-action`.

## Run ingestion from the UI
1. Launch the stack with `make up` and wait for `datahub-gms`, `datahub-frontend`, `kafka-autoheal`, and `ui-ingestion-runner` to report healthy.
2. Open [http://localhost:9002](http://localhost:9002) → Settings → Ingestion → **New source**.
3. Choose the **Postgres** recipe, supply database connection details reachable from the Docker network (`postgres:5432` by default), and save.
4. From the source details page click **Run** → **Trigger now**. Within ~15 seconds the run appears under **Runs** with status `RUNNING`.
5. Tail logs with `docker compose logs -f ui-ingestion-runner datahub-gms base64-action`. You should see:
   ```text
   ui-ingestion-runner | INFO Detected pending execution urn:li:dataHubExecutionRequest:123...
   ui-ingestion-runner | INFO Prepared recipe for ... (pipeline=ui-...)
   ui-ingestion-runner | INFO Classifier + encoder finished for pipeline ui-... (tables processed=1)
   base64-action       | INFO Encoding completed for 1 table(s) using explicit column overrides
   ```
6. When the run finishes, the UI run record transitions to **COMPLETED**. The Base64 action writes results into the `encoded.*` tables using either the UI-provided column list or the classifier output.

Troubleshooting tips:
- Ensure the UI runner resolves `datahub-gms` instead of `localhost`. Set `DATAHUB_GMS_URI=http://datahub-gms:8080` inside Docker or `http://localhost:8080` on the host. Include `DATAHUB_TOKEN`/`DATAHUB_ACTOR` if GMS enforces auth.
- If the run stalls with `PENDING`, confirm the Kafka topics exist (`docker compose run --rm kafka-setup`) and that the `kafka-autoheal` job succeeds.
- Permission errors indicate the runner's actor lacks `MANAGE_METADATA_INGESTION` or `MANAGE_SECRETS`. Grant them in DataHub or adjust the `DATAHUB_ACTOR` value.
- Recipe connection issues almost always stem from `localhost` being unreachable from the container. Replace it with `postgres` or `host.docker.internal` depending on where Postgres lives.

## Runner health check
- The UI ingestion runner now resolves the GMS endpoint with a sequence of lightweight probes. It will try `/api/health`, `/admin`,
  `/api/graphiql`, `/api/graphql` (GraphQL introspection), `/actuator/health`, and `/health` until one responds with HTTP 200. A
  successful GraphQL introspection is always required so that UI-triggered runs can proceed without false negatives.
- Override the defaults by exporting values (or updating [`.env.example`](.env.example)):
  ```bash
  DATAHUB_GMS_URI=http://datahub-gms:8080      # inside Docker Compose
  DATAHUB_GMS_URI=http://localhost:8080        # runner on your host machine
  DATAHUB_TOKEN=your_personal_access_token     # optional bearer token
  DATAHUB_ACTOR=urn:li:corpuser:alice          # defaults to ui_ingestion_runner
  HEALTH_CHECK_PATHS=/admin,/api/graphiql,/api/graphql,/actuator/health,/health
  ```
- When the runner starts it logs the resolved URI, each probe attempt (status code + first 200 response characters), and the
  endpoint that ultimately proved readiness. If every fallback fails the logs list all attempted URLs so you can diagnose which
  path (or authentication header) needs to be adjusted.

## PII Classifier & Trigger
- **Flow**: ingestion completes → classifier pulls tables from DataHub → regex rules mark PII → Base64 encoder rewrites flagged
  columns into `encoded.<table>` with idempotent inserts.
- **Configuration**: `actions/pii_classifier/config.yml` controls platform, default pipeline filter, sample size, and minimum
  value matches. Override at runtime with:
  - `PII_SAMPLE_ROWS` – rows to sample per table (default `200`).
  - `PII_MIN_MATCHES` – minimum regex matches before a value-only rule fires (default `5`).
  - `PIPELINE_NAME_FILTER` – skip events from other pipelines.
- **Patterns** live in [`classifier/patterns.yml`](classifier/patterns.yml). Each rule offers a `column` regex and optional
  `value` regex. Update patterns and re-run the classifier; no rebuild required.
- **Logs** clearly report progress:
  ```text
  pii-flow      | INFO Scanning 3 table(s) for PII using 4 regex rules
  pii-flow      | INFO Table public.customers flagged columns: email, phone_number, reference_code
  base64-action | INFO Encoding dataset urn:li:dataset:(...,public.customers,PROD) => public.customers (columns: email, phone_number, reference_code)
  base64-action | INFO Finished encoding urn:li:dataset:(...,public.customers,PROD) (42 rows)
  ```

## Ingestion helpers
- `make up` / `make down` – start or stop the quickstart stack.
- `docker compose run --rm ingestion` – run the CLI recipe at [`ingest/recipe.yml`](ingest/recipe.yml).
- `docker compose logs -f ui-ingestion-runner` – watch UI-triggered ingestions and classifier hand-offs.

## Testing & CI
- `pytest -vv` runs unit tests (regex rules) plus an end-to-end test that seeds Postgres, executes ingestion, runs the classifier,
  and validates the `encoded` schema. The e2e path assumes Docker and the stack are available.
- GitHub Actions workflow [`ci.yml`](.github/workflows/ci.yml) builds the helper images, starts the stack via `docker compose`,
  runs ingestion, executes the classifier + encoder, and finishes with the full pytest suite.

## Kafka setup & health
- **Bootstrap topics and schemas** once the stack is up:
  ```bash
  docker compose run --rm kafka-setup
  ```
  The job is idempotent. It creates all required DataHub topics with a single partition/replica and registers the matching Avro
  schemas with Schema Registry.
- **Auto-heal check** (re)runs the same logic and fails fast if a topic or subject disappears:
  ```bash
  docker compose run --rm kafka-autoheal
  ```
- **Verify broker state** from your host:
  ```bash
  docker compose exec -T kafka \
    /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
  ```
  Expected topics: `MetadataChangeEvent_v4`, `FailedMetadataChangeEvent_v4`, `MetadataAuditEvent_v4`,
  `FailedMetadataAuditEvent_v4`, `DataHubUsageEvent_v1`, `PlatformEvent_v1`.
- **Tail consumer logs** if ingestion ever stalls:
  ```bash
  docker compose logs -f datahub-gms datahub-mce-consumer datahub-mae-consumer | grep -i unknown_topic || true
  ```
- **Common fixes**: double-check that containers can resolve `kafka` and `schema-registry`; expose ports `9092` and `8081` if you
  connect from your host; keep `replication.factor=1` for the single-broker PoC (bumping it requires a multi-broker cluster).

## Tweaking the classifier quickly
1. Edit `classifier/patterns.yml` to add or adjust regexes.
2. Optionally bump `PII_SAMPLE_ROWS` / `PII_MIN_MATCHES` for noisier datasets.
3. Re-run `python -m scripts.run_classifier_and_encode ...` (or trigger an ingestion through the UI). Encoded tables refresh in
   place without duplicating rows.
