import types

import pytest

pytest.importorskip("requests")

from tools.verify_poc import POCVerifier, VerificationError


class DummyResponse:
    def __init__(self, *, status_code=200, json_payload=None, text="", raise_for_status=None):
        self.status_code = status_code
        self._json_payload = json_payload or {}
        self.text = text
        self._raise_for_status = raise_for_status

    def json(self):
        return self._json_payload

    def raise_for_status(self):
        if self._raise_for_status:
            raise self._raise_for_status


def make_verifier(tmp_path):
    return POCVerifier(
        namespace="datahub",
        tenant="t001",
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,sandbox.t001.customers,PROD)",
        timeout=5,
        artifacts_dir=tmp_path,
        expect_idempotent=True,
        request_id="test",
    )


def test_http_helpers_success(monkeypatch, tmp_path):
    verifier = make_verifier(tmp_path)

    monkeypatch.setattr("requests.get", lambda url, timeout=10: DummyResponse(status_code=200))

    resp = verifier._http_get("http://example.com")
    assert resp.status_code == 200

    def fake_post(url, json, timeout):
        return DummyResponse(json_payload={"data": {"health": {"status": "HEALTHY"}}})

    monkeypatch.setattr("requests.post", fake_post)
    payload = verifier._http_post("http://example.com", json_payload={})
    assert payload["data"]["health"]["status"] == "HEALTHY"


def test_http_helpers_error(monkeypatch, tmp_path):
    verifier = make_verifier(tmp_path)

    def raising_get(url, timeout=10):
        raise RuntimeError("boom")

    monkeypatch.setattr("requests.get", raising_get)
    with pytest.raises(VerificationError):
        verifier._http_get("http://example.com")

    class DummyExc(Exception):
        pass

    monkeypatch.setattr(
        "requests.post",
        lambda url, json, timeout: DummyResponse(raise_for_status=DummyExc("bad")),
    )
    with pytest.raises(VerificationError):
        verifier._http_post("http://example.com", json_payload={})
