"""Live fail-hard tests against official STN SICONFI REST API endpoints."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
    SICONFI_ENTES_URL,
    StnSiconfiConnector,
)


@pytest.mark.live
def test_stn_siconfi_live_smoke_connection() -> None:
    """Verify live connectivity against SICONFI /entes endpoint and fail hard on HTTP 5xx."""
    conn = StnSiconfiConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["total_items_page"] > 0
    assert report["sample_cod_ibge"]


@pytest.mark.live
def test_stn_siconfi_live_entes_schema() -> None:
    """Verify official SICONFI /entes API returns Brazilian municipality rows with IBGE codes."""
    resp = httpx.get(SICONFI_ENTES_URL, timeout=20.0)
    assert resp.status_code == 200, f"SICONFI /entes returned HTTP {resp.status_code}"

    data = resp.json()
    items = data.get("items", [])
    assert len(items) > 0, "SICONFI /entes must return non-empty items"
    sample = items[0]
    assert "cod_ibge" in sample
    assert "ente" in sample
    assert "uf" in sample
