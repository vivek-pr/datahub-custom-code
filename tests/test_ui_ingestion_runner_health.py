from __future__ import annotations

import json
from typing import Any, Dict, Optional

import pytest
import requests

from scripts import ui_ingestion_runner as runner


class MockResponse:
    def __init__(
        self,
        status_code: int,
        text: str = "",
        json_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        if text:
            self.text = text
        elif json_data is not None:
            self.text = json.dumps(json_data)
        else:
            self.text = ""

    def json(self) -> Dict[str, Any]:
        if self._json_data is None:
            raise ValueError("No JSON payload configured")
        return self._json_data


def _build_graphql_response() -> MockResponse:
    payload = {"data": {"__schema": {"queryType": {"name": "Query"}}}}
    return MockResponse(200, json_data=payload)


def test_health_check_falls_back_to_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    base_url = "http://datahub-gms:8080"
    session = requests.Session()

    responses: Dict[str, MockResponse] = {
        f"{base_url}/api/health": MockResponse(404, text="not found"),
        f"{base_url}/admin": MockResponse(200, text="ok"),
        f"{base_url}/api/graphql": _build_graphql_response(),
    }

    call_log: list[tuple[str, str]] = []

    def fake_request(method: str, url: str, **kwargs: Any) -> MockResponse:
        call_log.append((method.upper(), url))
        response = responses.get(url)
        if response is None:
            raise AssertionError(f"Unexpected URL requested: {url}")
        return response

    monkeypatch.setattr(session, "request", fake_request)
    monkeypatch.setattr(runner, "GMS_URL_ENV", base_url)
    monkeypatch.setattr(runner, "GMS_URL", base_url)
    monkeypatch.setattr(runner, "DATAHUB_TOKEN", None)
    monkeypatch.setattr(runner, "HEALTH_CHECK_PATHS", [
        "/api/health",
        "/admin",
        "/api/graphql",
    ])

    resolved = runner.resolve_gms_url(session, {})

    assert resolved == base_url
    assert ("GET", f"{base_url}/api/health") in call_log
    assert ("GET", f"{base_url}/admin") in call_log
    assert ("POST", f"{base_url}/api/graphql") in call_log


def test_health_check_accepts_graphql_when_rest_endpoints_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_url = "http://datahub-gms:8080"
    session = requests.Session()

    responses: Dict[str, MockResponse] = {
        f"{base_url}/api/health": MockResponse(404, text="missing"),
        f"{base_url}/admin": MockResponse(404, text="missing"),
        f"{base_url}/api/graphiql": MockResponse(404, text="missing"),
        f"{base_url}/api/graphql": _build_graphql_response(),
    }

    def fake_request(method: str, url: str, **kwargs: Any) -> MockResponse:
        response = responses.get(url)
        if response is None:
            raise AssertionError(f"Unexpected URL requested: {url}")
        return response

    monkeypatch.setattr(session, "request", fake_request)
    monkeypatch.setattr(runner, "GMS_URL_ENV", base_url)
    monkeypatch.setattr(runner, "GMS_URL", base_url)
    monkeypatch.setattr(runner, "DATAHUB_TOKEN", None)
    monkeypatch.setattr(runner, "HEALTH_CHECK_PATHS", [
        "/api/health",
        "/admin",
        "/api/graphiql",
        "/api/graphql",
    ])

    resolved = runner.resolve_gms_url(session, {})

    assert resolved == base_url
