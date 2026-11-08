"""Live 1-line real API test suite for BACEN SGS Macroeconomic connector.

Enforces the Fail-Hard Policy requested by user:
1. Every official endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard.
"""

from __future__ import annotations

import pytest

from brazil_data_gateway.connectors.macrofinanceiro.bcb_sgs import (
    BCBSGSConnector,
)


@pytest.fixture
def live_connector() -> BCBSGSConnector:
    """Instantiate connector with live network enabled."""
    return BCBSGSConnector()


@pytest.mark.live
def test_live_connection_bcb_sgs(live_connector: BCBSGSConnector) -> None:
    """Connection check against BACEN SGS API must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to BACEN SGS API failed"


@pytest.mark.live
def test_live_1line_fail_hard_all_bcb_sgs_endpoints(live_connector: BCBSGSConnector) -> None:
    """Verify all 7 official SGS endpoints live with 1-line limit. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert len(results) == 7, f"Expected 7 live endpoint checks, got {len(results)}"
    for table_name, passed in results.items():
        assert passed is True, f"Live endpoint test for '{table_name}' failed unexpectedly"


def test_live_fail_hard_simulation(live_connector: BCBSGSConnector, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate government API disruption and verify the test fails hard with RuntimeError."""
    class MockFailedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 503
                text = "Service Unavailable (Simulated BACEN Outage)"

                def json(self):
                    return {}

            return MockResponse()

    monkeypatch.setattr(live_connector, "http_client", MockFailedClient())

    with pytest.raises(RuntimeError, match="Fail-Hard"):
        live_connector.test_live_endpoints(fail_hard=True)
