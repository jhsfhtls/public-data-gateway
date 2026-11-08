"""Live 1-line real API test suite for Receita Federal CNPJ Lakehouse connector.

Enforces the Fail-Hard Policy requested by user:
1. The official CNPJ endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard.
"""

from __future__ import annotations

import pytest

from brazil_data_gateway.connectors.empresarial.rfb_cnpj_lakehouse import (
    RFBCNPJLakehouseConnector,
)


@pytest.fixture
def live_connector() -> RFBCNPJLakehouseConnector:
    """Instantiate connector with live network enabled."""
    return RFBCNPJLakehouseConnector()


@pytest.mark.live
def test_live_connection_rfb_cnpj(live_connector: RFBCNPJLakehouseConnector) -> None:
    """Connection check against CNPJ resolution edge service must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to CNPJ resolution edge service failed"


@pytest.mark.live
def test_live_1line_fail_hard_rfb_cnpj(live_connector: RFBCNPJLakehouseConnector) -> None:
    """Verify official CNPJ endpoint live with 1-line limit. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert results.get("cnpj_live") is True, "Live CNPJ endpoint test failed unexpectedly"


def test_live_fail_hard_simulation(live_connector: RFBCNPJLakehouseConnector, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate government/edge API disruption and verify the test fails hard with RuntimeError."""

    class MockFailedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 503
                text = "Service Unavailable (Simulated RFB CNPJ Outage)"

                def json(self):
                    return {"error": "Outage"}

            return MockResponse()

    monkeypatch.setattr(
        "brazil_data_gateway.connectors.empresarial.rfb_cnpj_lakehouse.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockFailedClient(),
    )

    with pytest.raises(RuntimeError) as exc_info:
        live_connector.test_live_endpoints(fail_hard=True)

    assert "[FAIL-HARD]" in str(exc_info.value)
    assert "503" in str(exc_info.value)


def test_live_fail_hard_schema_drift_simulation(
    live_connector: RFBCNPJLakehouseConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulate schema drift in upstream API and verify the test fails hard with RuntimeError."""

    class MockDriftedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 200

                def json(self):
                    # Schema drift: cnpj field missing
                    return {"unexpected_field": "test_value"}

            return MockResponse()

    monkeypatch.setattr(
        "brazil_data_gateway.connectors.empresarial.rfb_cnpj_lakehouse.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockDriftedClient(),
    )

    with pytest.raises(RuntimeError) as exc_info:
        live_connector.test_live_endpoints(fail_hard=True)

    assert "[FAIL-HARD]" in str(exc_info.value)
    assert "schema drift" in str(exc_info.value)
