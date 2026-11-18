"""Live fail-hard tests against official Tesouro Transparente CKAN CAPAG endpoints."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
    STN_CAPAG_CKAN_URL,
    StnCapagConnector,
)


@pytest.mark.live
def test_stn_capag_live_smoke_connection() -> None:
    """Verify live connectivity against Tesouro Transparente CKAN CAPAG API and fail hard on HTTP 5xx."""
    conn = StnCapagConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["packages_found"] > 0


@pytest.mark.live
def test_stn_capag_live_ckan_package_search() -> None:
    """Verify Tesouro Transparente CKAN CAPAG search returns valid JSON datasets."""
    resp = httpx.get(
        STN_CAPAG_CKAN_URL,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, f"CKAN CAPAG returned HTTP {resp.status_code}"
    payload = resp.json()
    assert payload.get("success") is True
    assert len(payload.get("result", {}).get("results", [])) > 0
