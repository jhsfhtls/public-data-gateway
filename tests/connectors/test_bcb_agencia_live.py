"""Live fail-hard tests against official BACEN Olinda Informes_Agencias OData API."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
    BCB_AGENCIAS_ODATA_URL,
    BcbAgenciaConnector,
)


@pytest.mark.live
def test_bcb_agencia_live_smoke_connection() -> None:
    """Verify live connectivity against BACEN Olinda Agencias API and fail hard on HTTP 5xx."""
    conn = BcbAgenciaConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["rows_returned"] >= 1


@pytest.mark.live
def test_bcb_agencia_live_odata_get() -> None:
    """Verify official BACEN Olinda Informes_Agencias endpoint responds HTTP 200 with rows."""
    resp = httpx.get(
        BCB_AGENCIAS_ODATA_URL,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, f"BACEN Olinda Agencias returned HTTP {resp.status_code}"
    payload = resp.json()
    assert len(payload.get("value", [])) >= 1
