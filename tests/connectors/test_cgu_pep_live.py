"""Live 1-line real API test suite for CGU PEP (Pessoas Expostas Politicamente) connector.

Enforces the Fail-Hard Policy requested by user:
1. The official CGU Portal da Transparência OpenAPI endpoint is queried with a live 1-line query.
2. If the API returns non-200, 429, or altered schema, the test MUST fail hard with RuntimeError.
3. If CGU_API_KEY is configured, also verifies the live authenticated PEP endpoint.
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.compliance.cgu_pep import (
    CGUPEPConnector,
)


@pytest.fixture
def live_connector() -> CGUPEPConnector:
    """Instantiate connector with live network enabled."""
    return CGUPEPConnector()


@pytest.mark.live
def test_live_connection_cgu_pep(live_connector: CGUPEPConnector) -> None:
    """Connection check against CGU Portal da Transparência API must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to CGU PEP API failed"


@pytest.mark.live
def test_live_1line_fail_hard_cgu_pep(live_connector: CGUPEPConnector) -> None:
    """Verify official CGU endpoint live with 1-line query. Must fail hard on any disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert results.get("cgu_docs_live") is True, "Live CGU PEP endpoint test failed unexpectedly"


def test_live_fail_hard_simulation_cgu_pep(
    live_connector: CGUPEPConnector, monkeypatch: pytest.MonkeyPatch
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
                text = "Service Unavailable (Simulated CGU Outage)"

                def json(self):
                    return {"error": "Outage"}

            return MockResponse()

    monkeypatch.setattr(
        "public_data_gateway.connectors.compliance.cgu_pep.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockFailedClient(),
    )

    with pytest.raises(RuntimeError, match=r"\[FAIL-HARD\]"):
        live_connector.test_live_endpoints(fail_hard=True)
