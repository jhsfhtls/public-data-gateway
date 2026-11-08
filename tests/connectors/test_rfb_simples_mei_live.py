"""Live 1-line real API test suite for Receita Federal Simples Nacional & MEI connector.

Enforces the Fail-Hard Policy requested by user:
1. The official Simples/CNPJ endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard.
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.empresarial.rfb_simples_mei import (
    RFBSimplesMEIConnector,
)


@pytest.fixture
def live_connector() -> RFBSimplesMEIConnector:
    """Instantiate connector with live network enabled."""
    return RFBSimplesMEIConnector()


@pytest.mark.live
def test_live_connection_rfb_simples(live_connector: RFBSimplesMEIConnector) -> None:
    """Connection check against Simples/MEI resolution edge service must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to Simples edge service failed"


@pytest.mark.live
def test_live_1line_fail_hard_rfb_simples(live_connector: RFBSimplesMEIConnector) -> None:
    """Verify official Simples/MEI endpoint live with 1-line limit. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert results.get("simples_live") is True, "Live Simples endpoint test failed unexpectedly"


def test_live_fail_hard_simulation(live_connector: RFBSimplesMEIConnector, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate government/edge API disruption and verify the test fails hard with RuntimeError."""

    class MockFailedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 503
                text = "Service Unavailable (Simulated RFB Simples Outage)"

                def json(self):
                    return {"error": "Outage"}

            return MockResponse()

    monkeypatch.setattr(
        "public_data_gateway.connectors.empresarial.rfb_simples_mei.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockFailedClient(),
    )

    with pytest.raises(RuntimeError, match=r"\[FAIL-HARD\]"):
        live_connector.test_live_endpoints(fail_hard=True)
