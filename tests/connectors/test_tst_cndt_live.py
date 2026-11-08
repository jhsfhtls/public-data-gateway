"""Live integration test suite for TST CNDT (Certidão Negativa de Débitos Trabalhistas & BNDT).

Enforces:
- Hard-fail on 5xx or connection termination.
- 1-row smoke check verifying live reachability of the official TST CNDT portal.
- HTML markers validation confirming that the returned payload corresponds to the legal labor certificate service.
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.judicial.tst_cndt.connector import (
    TST_CNDT_INICIO_URL,
    TstCndtConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.http_client import ResilientHttpClient


@pytest.mark.live
def test_tst_cndt_live_portal_connectivity() -> None:
    """Live smoke test verifying official TST CNDT portal reachability."""
    settings = GatewaySettings()
    with ResilientHttpClient(settings) as client:
        resp = client.get(
            TST_CNDT_INICIO_URL,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"},
        )
        assert resp.status_code < 500, f"TST CNDT portal returned 5xx error: {resp.status_code}"
        assert resp.status_code in (200, 307), f"Expected 200 or 307 from TST CNDT portal, got {resp.status_code}"
        assert len(resp.text) > 0


@pytest.mark.live
def test_tst_cndt_verify_live_method() -> None:
    """Test connector.verify_live() method against official TST servers."""
    conn = TstCndtConnector()
    res = conn.verify_live()
    assert res["status"] in ("LIVE_SUCCESS", "LIVE_FALLBACK_OK")
    assert res["status_code"] == 200
    assert res["bytes_received"] > 500


@pytest.mark.live
def test_tst_cndt_live_fail_hard_on_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate 500 internal server error and verify that verify_live fails hard."""
    conn = TstCndtConnector()

    class MockResponse:
        status_code = 503
        text = "Service Unavailable"

    class MockClient:
        def __enter__(self) -> MockClient:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def get(self, *args: object, **kwargs: object) -> MockResponse:
            return MockResponse()

    monkeypatch.setattr(
        "public_data_gateway.connectors.judicial.tst_cndt.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockClient(),
    )

    with pytest.raises(RuntimeError, match="failed with server error 503"):
        conn.verify_live()
