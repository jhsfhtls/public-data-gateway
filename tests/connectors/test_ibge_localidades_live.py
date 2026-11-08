"""Live 1-line real API test suite for IBGE Localidades connector.

Enforces the Fail-Hard Policy requested by user:
1. Every official endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard.
"""

from __future__ import annotations

import pytest

from brazil_data_gateway.connectors.demografico.ibge_localidades import (
    IBGELocalidadesConnector,
)


@pytest.fixture
def live_connector() -> IBGELocalidadesConnector:
    """Instantiate connector with live network enabled."""
    return IBGELocalidadesConnector()


@pytest.mark.live
def test_live_connection_ibge(live_connector: IBGELocalidadesConnector) -> None:
    """Connection check against IBGE API must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to IBGE API failed"


@pytest.mark.live
def test_live_1line_fail_hard_all_5_endpoints(live_connector: IBGELocalidadesConnector) -> None:
    """Verify all 5 endpoints live with 1-line limit. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert len(results) == 5, f"Expected 5 live endpoint checks, got {len(results)}"
    for table_name, passed in results.items():
        assert passed is True, f"Live endpoint test for '{table_name}' failed unexpectedly"


def test_live_fail_hard_simulation(live_connector: IBGELocalidadesConnector, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate government API disruption and verify the test fails hard with RuntimeError."""
    class MockFailedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 503
                text = "Service Unavailable (Simulated Outage)"

                def json(self):
                    return {}

            return MockResponse()

    monkeypatch.setattr(live_connector, "http_client", MockFailedClient())

    with pytest.raises(RuntimeError, match="Fail-Hard"):
        live_connector.test_live_endpoints(fail_hard=True)
