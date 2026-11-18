"""Live fail-hard tests against official IBGE SIDRA PAM / PPM / PEVS APIs."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
    IBGE_PEVS_SIDRA_URL,
    IBGE_PPM_SIDRA_URL,
    IbgeAgroMunicipalConnector,
)


@pytest.mark.live
def test_ibge_agro_municipal_live_smoke_connection() -> None:
    """Verify live connectivity against IBGE SIDRA PAM Tabela 5457 API."""
    conn = IbgeAgroMunicipalConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["rows"] >= 2


@pytest.mark.live
def test_ibge_agro_municipal_live_ppm_and_pevs() -> None:
    """Verify live IBGE SIDRA PPM Tabela 3939 and PEVS Tabela 289 endpoints return HTTP 200."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    for url in (IBGE_PPM_SIDRA_URL, IBGE_PEVS_SIDRA_URL):
        resp = httpx.get(
            url,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
            verify=False,
        )
        assert resp.status_code == 200, f"IBGE SIDRA returned HTTP {resp.status_code} for {url}"
        rows = resp.json()
        assert isinstance(rows, list) and len(rows) >= 2
