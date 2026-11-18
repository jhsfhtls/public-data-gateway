"""Live fail-hard tests against official BACEN Olinda SICOR OData v2 API."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.bcb_sicor import (
    BCB_SICOR_ODATA_ROOT_URL,
    BcbSicorConnector,
)


@pytest.mark.live
def test_bcb_sicor_live_smoke_connection() -> None:
    """Verify live connectivity against BACEN Olinda SICOR OData Custeio API."""
    conn = BcbSicorConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["record_count"] >= 1


@pytest.mark.live
def test_bcb_sicor_live_odata_service_root() -> None:
    """Verify BACEN Olinda SICOR OData v2 service root advertises Custeio & Investimento."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    resp = httpx.get(
        BCB_SICOR_ODATA_ROOT_URL,
        headers=headers,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, (
        f"BACEN SICOR OData root returned HTTP {resp.status_code}"
    )
    payload = resp.json()
    entity_names = {item.get("name") for item in payload.get("value", [])}
    assert {"CusteioMunicipioProduto", "InvestMunicipioProduto"}.issubset(entity_names)
