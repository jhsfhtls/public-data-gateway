"""Live 1-line real API test suite for BrasilAPI Core Edge Services connector.

Enforces the Fail-Hard Policy requested by user:
1. Every official endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard.
"""

from __future__ import annotations

import pytest

from brazil_data_gateway.connectors.agregadores.brasilapi_core import (
    BrasilAPICoreConnector,
)


@pytest.fixture
def live_connector() -> BrasilAPICoreConnector:
    """Instantiate connector with live network enabled."""
    return BrasilAPICoreConnector()


@pytest.mark.live
def test_live_connection_brasilapi_core(live_connector: BrasilAPICoreConnector) -> None:
    """Connection check against BrasilAPI root service must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to BrasilAPI root service failed"


@pytest.mark.live
def test_live_1line_fail_hard_all_brasilapi_endpoints(live_connector: BrasilAPICoreConnector) -> None:
    """Verify official BrasilAPI endpoints live with 1-line limit. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert len(results) == 6, f"Expected 6 live endpoint checks, got {len(results)}"
    for table_name, passed in results.items():
        assert passed is True, f"Live endpoint test for '{table_name}' failed unexpectedly"


def test_live_fail_hard_simulation(live_connector: BrasilAPICoreConnector, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate government/edge API disruption and verify the test fails hard with RuntimeError."""

    class MockFailedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 503
                text = "Service Unavailable (Simulated BrasilAPI Outage)"

                def json(self):
                    return {"error": "Outage"}

            return MockResponse()

    monkeypatch.setattr(
        "brazil_data_gateway.connectors.agregadores.brasilapi_core.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockFailedClient(),
    )

    with pytest.raises(RuntimeError) as exc_info:
        live_connector.test_live_endpoints(fail_hard=True)

    assert "FAIL-HARD" in str(exc_info.value)
    assert "503" in str(exc_info.value)
