"""Live 1-line real API test suite for BACEN Olinda PTAX Foreign Exchange connector.

Enforces the Fail-Hard Policy requested by user:
1. Every official endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard.
"""

from __future__ import annotations

import pytest

from brazil_data_gateway.connectors.macrofinanceiro.bcb_olinda_ptax import (
    BCBOlindaPTAXConnector,
)


@pytest.fixture
def live_connector() -> BCBOlindaPTAXConnector:
    """Instantiate connector with live network enabled."""
    return BCBOlindaPTAXConnector()


@pytest.mark.live
def test_live_connection_bcb_olinda_ptax(live_connector: BCBOlindaPTAXConnector) -> None:
    """Connection check against BACEN Olinda PTAX API must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to BACEN Olinda PTAX API failed"


@pytest.mark.live
def test_live_1line_fail_hard_all_olinda_ptax_endpoints(live_connector: BCBOlindaPTAXConnector) -> None:
    """Verify official Olinda PTAX endpoints live with 1-line limit. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert len(results) == 3, f"Expected 3 live endpoint checks, got {len(results)}"
    for table_name, passed in results.items():
        assert passed is True, f"Live endpoint test for '{table_name}' failed unexpectedly"


def test_live_fail_hard_simulation(live_connector: BCBOlindaPTAXConnector, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate government API disruption and verify the test fails hard with RuntimeError."""
    class MockFailedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 503
                text = "Service Unavailable (Simulated Olinda Outage)"

                def json(self):
                    return {}

            return MockResponse()

    monkeypatch.setattr(live_connector, "http_client", MockFailedClient())

    with pytest.raises(RuntimeError, match="Fail-Hard"):
        live_connector.test_live_endpoints(fail_hard=True)
