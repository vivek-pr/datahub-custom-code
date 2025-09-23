"""Tests for the tokenization run status payload."""

from __future__ import annotations

import json
from datetime import datetime

from action.models import RunStatus


def test_run_status_serialises_to_json() -> None:
    started = datetime(2024, 1, 1, 12, 0, 0)
    ended = datetime(2024, 1, 1, 12, 5, 0)
    status = RunStatus(
        run_id="abc",
        started_at=started,
        ended_at=ended,
        platform="postgres",
        columns=["email", "phone"],
        rows_updated=42,
        rows_skipped=8,
        status="SUCCESS",
        message="Updated rows successfully",
    )
    payload = status.dict()
    payload["started_at"] = status.started_at.isoformat()
    payload["ended_at"] = status.ended_at.isoformat() if status.ended_at else None
    blob = json.dumps({"last_tokenization_run": payload})
    decoded = json.loads(blob)["last_tokenization_run"]
    assert decoded["status"] == "SUCCESS"
    assert decoded["rows_updated"] == 42
    assert decoded["columns"] == ["email", "phone"]
    assert decoded["started_at"].startswith("2024-01-01T12:00:00")
