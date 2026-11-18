"""Live fail-hard tests against official IBAMA Open Data SIFISC endpoints."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
    IBAMA_CKAN_AUTOS_URL,
    IBAMA_CKAN_EMBARGOS_URL,
    IbamaFiscalizacaoConnector,
)


@pytest.mark.live
def test_ibama_fiscalizacao_live_smoke_connection() -> None:
    """Verify live connectivity against IBAMA CKAN SIFISC API and fail hard on HTTP 5xx."""
    conn = IbamaFiscalizacaoConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["resource_count"] >= 1


@pytest.mark.live
def test_ibama_fiscalizacao_live_embargos_ckan() -> None:
    """Verify IBAMA Termos de Embargo CKAN metadata endpoint responds HTTP 200 with resources."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    resp = httpx.get(
        IBAMA_CKAN_EMBARGOS_URL,
        headers=headers,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, (
        f"IBAMA CKAN embargos returned HTTP {resp.status_code} ({IBAMA_CKAN_AUTOS_URL})"
    )
    payload = resp.json()
    assert payload.get("success") is True
    assert len(payload.get("result", {}).get("resources", [])) >= 1
