# ADR-0001: Approach-1 — Governance Bootstrap

- Status: Accepted
- Date: 2025-09-17

## Context

We need a simple, reliable governance baseline to ensure quality from the outset without over-investing in language- or stack-specific tooling. The repository is currently empty of runtime code, so checks should focus on hygiene, docs, and CI that scales as code is introduced.

## Decision

Adopt a lightweight, language-agnostic approach centered on:

- Pre-commit hooks for fast, local hygiene checks (whitespace, EOF, YAML/JSON/TOML syntax, private keys, large files) and spelling (`codespell`).
- GitHub Actions CI that runs pre-commit on every push/PR and a simple smoke script validating required docs and configs exist.
- Clear documentation of scope/constraints (`docs/00_overview.md`) and this ADR (`docs/decisions.md`).

This balances developer experience and governance without presupposing a runtime.

## Consequences

- Pros
  - Fast feedback locally and in CI.
  - Minimal dependencies; easy onboarding.
  - Extensible: language-specific linters/formatters can be added later.

- Cons
  - Limited depth vs. language-specific static analysis or formatters.
  - Some checks (e.g., security scanning) deferred to future iterations.

## Alternatives Considered

1) Heavy language-specific toolchains now (e.g., Python `ruff`/`black`, JS `eslint`/`prettier`)
   - Rejected for now: premature without runtime code; adds friction.

2) CI-only checks (no pre-commit)
   - Rejected: slower feedback; higher PR churn.

3) Do nothing until code exists
   - Rejected: loses early consistency and baseline quality.
