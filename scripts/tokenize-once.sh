#!/usr/bin/env bash
set -euo pipefail

# Helper to run the Base64 action once on demand. Useful for manual triggers after
# a UI-driven ingestion completes.
${DC:-docker compose} exec base64-action python -u action.py --once
