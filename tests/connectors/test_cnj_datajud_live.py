"""Live 1-line real API test suite for CNJ DataJud (Base Nacional do Poder Judiciário) connector.

Enforces the Fail-Hard Policy requested by user:
1. The official CNJ DataJud endpoint/documentation is queried with a live 1-line query.
2. If the server returns unexpected 5xx status codes, the test MUST fail hard with RuntimeError.
3. Simulates edge outage to confirm fail-hard enforcement.
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.judicial.cnj_datajud import (
    CNJDataJudConnector,
)


@pytest.fixture
def live_connector() -> CNJDataJudConnector:
    """Instantiate connector with live network enabled."""
    return CNJDataJudConnector()


@pytest.mark.live
def test_live_connection_cnj_datajud(live_connector: CNJDataJudConnector) -> None:
    """Connection check against CNJ DataJud service must succeed live."""
    assert live_connector.test_connection() is True, "Live connection to CNJ DataJud failed"


@pytest.mark.live
def test_live_1line_fail_hard_cnj_datajud(live_connector: CNJDataJudConnector) -> None:
    """Verify official CNJ DataJud endpoints live with 1-line query. Must fail hard on disruption."""
    results = live_connector.test_live_endpoints(fail_hard=True)
    assert results.get("datajud_wiki") is True, "Live CNJ DataJud wiki test failed unexpectedly"
    assert results.get("datajud_api_live") is True, "Live CNJ DataJud cluster check failed unexpectedly"


def test_live_fail_hard_simulation_cnj_datajud(
    live_connector: CNJDataJudConnector, monkeypatch: pytest.MonkeyPatch
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
                text = "Service Unavailable (Simulated CNJ Outage)"

                def json(self):
                    return {"error": "Outage"}

            return MockResponse()

    monkeypatch.setattr(
        "public_data_gateway.connectors.judicial.cnj_datajud.connector.ResilientHttpClient",
        lambda *args, **kwargs: MockFailedClient(),
    )

    with pytest.raises(RuntimeError, match=r"\[FAIL-HARD\]"):
        live_connector.test_live_endpoints(fail_hard=True)
