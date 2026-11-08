"""Live 1-line real API test suite for PNCP Contratações e Licitações connector.

Enforces the Fail-Hard Policy requested by user:
1. The official PNCP Consulta endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard.
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
    PNCPContratacoesConnector,
)


@pytest.fixture
def live_connector() -> PNCPContratacoesConnector:
    """Instantiate connector with live network enabled."""
    return PNCPContratacoesConnector()


@pytest.mark.live
def test_live_connection_pncp(live_connector: PNCPContratacoesConnector) -> None:
    """Connection check against PNCP Consulta service must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to PNCP service failed"


@pytest.mark.live
def test_live_1line_fail_hard_pncp(live_connector: PNCPContratacoesConnector) -> None:
    """Verify official PNCP endpoint live with 1-line query. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert results.get("pncp_live") is True, "Live PNCP endpoint test failed unexpectedly"


def test_live_fail_hard_simulation(
    live_connector: PNCPContratacoesConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulate government/edge API disruption and verify the test fails hard with RuntimeError."""

    class MockFailedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            class MockResponse:
                status_code = 503
                text = "Service Unavailable (Simulated PNCP Outage)"

                def json(self):
                    return {"error": "Outage"}

            return MockResponse()

    monkeypatch.setattr(
        "public_data_gateway.connectors.compras_publicas.pncp_contratacoes.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockFailedClient(),
    )

    with pytest.raises(RuntimeError, match=r"\[FAIL-HARD\] PNCP live endpoint returned HTTP 503"):
        live_connector.test_live_endpoints(fail_hard=True)
