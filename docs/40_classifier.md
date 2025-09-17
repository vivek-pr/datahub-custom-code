Regex PII Classifier

- Why: Auto-tag schema fields with pii-* tags so the DataHub UI and Actions can target sensitive data.
- What: Python service (`services/pii-classifier`) that inspects Postgres schemas, samples values, applies regex/name heuristics, and emits DataHub tags via the REST emitter.

Inputs & Config

- Postgres connection: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` (env). Limit schemas with `POSTGRES_SCHEMAS` (comma-separated). Sampling limit via `POSTGRES_SAMPLE_LIMIT` (default 50) and `CLASSIFIER_MIN_VALUE_SAMPLES` (default 5).
- DataHub: `DATAHUB_GMS` (default `http://localhost:8080`), optional `DATAHUB_TOKEN`, `DATAHUB_PLATFORM` (default `postgres`), `DATAHUB_ENV` (default `PROD`). Set `CLASSIFIER_DRY_RUN=true` to log instead of emitting.
- Rules file: defaults to `sample/regex/rules.yml`; override with `CLASSIFIER_RULES_PATH` or `--rules` flag. Rules define tag URNs, column-name patterns, value regex, weights, and minimum confidence.

Rules (starter set)

- Email (`urn:li:tag:pii-email`): name pattern `email|e_mail`, value regex RFC-ish email, weight 45% name / 55% value, fires at ≥0.6 confidence.
- Phone (`urn:li:tag:pii-phone`): name `phone|mobile|msisdn|contact`, lenient Indian phone regex with optional +91, 0.6 confidence.
- PAN (`urn:li:tag:pii-pan`): name `pan|permanent_account`, value regex `[A-Z]{5}\d{4}[A-Z]`.
- Aadhaar (`urn:li:tag:pii-aadhaar`): name `aadhaar|aadhar`, value regex `^\d{12}$`, requires 70% value hits.
- Name (`urn:li:tag:pii-name`): name-based only (`name|full_name|first_name|last_name|fname|lname`) to catch personal names when sampling confirms ≥5 non-empty rows.
- Card (`urn:li:tag:pii-card`): name `card|cc|creditcard|debitcard`, lenient 12-19 digit regex.

Signals & Confidence

- Each rule assigns weights to name vs value evidence. Score = sum of weights that fire, capped at 1.0. A name-only rule reaches the rule’s `min_confidence` when the name pattern matches. Value evidence uses the share of non-empty samples matching the rule regex (defaults: ≥50%, Aadhaar 70%).
- Columns with fewer than `CLASSIFIER_MIN_VALUE_SAMPLES` non-empty samples are skipped to avoid noisy results.
- The service tracks existing schemaField tags and performs UPSERTs, avoiding duplicates for idempotent reruns.

Running the Classifier

1) Ensure Postgres sample data is loaded and ingested into DataHub (e.g., `make pg:up`, `make pg:ingest`).
2) Port-forward or expose GMS so the runner can reach `DATAHUB_GMS`.
3) Run `make classifier:run CLASSIFIER_ARGS='--schemas t001,t002 --dry-run'` (omit `--dry-run` to emit). The target bootstraps a venv, installs deps, and invokes `python -m pii_classifier.cli`.
4) Review stdout for matches; in non-dry runs, confirm tags in the DataHub UI under Dataset → Schema → Tags.

Testing & Verification

- Unit tests: `pytest tests/pii_classifier/test_rules.py` covers regex accuracy and scoring heuristics.
- Service tests: `pytest tests/pii_classifier/test_classifier.py` exercises the classifier pipeline with fakes to ensure tag emissions are attempted.
- Integration (optional): set `DATAHUB_GMS`, `DATAHUB_TOKEN`, and `CLASSIFIER_TEST_DATASET_URN`, then run `pytest -m integration tests/pii_classifier/test_graphql_integration.py` to assert a dataset exposes `pii-*` tags via GraphQL.
- E2E checklist: Postgres ingest → classifier run (non dry-run) → DataHub UI schema shows at least four expected pii-* tags. Reruns should produce “Tag already present” logs without duplicates.

Safety Notes

- Dry-run first to inspect planned tags before mutating DataHub metadata.
- Regexes are intentionally lenient; extend the rule set or tighten thresholds for production datasets.
- Maintain a reviewed allowlist of tag URNs; the classifier creates/ensures tag entities before applying them.
- Downstream Actions can filter on `pii-*` tags once these are attached; validate automation in staging before promoting.
