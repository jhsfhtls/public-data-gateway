"""Live fail-hard tests against official Secretaria do Tesouro Nacional (STN) endpoints."""

from __future__ import annotations

import pytest
import requests
import urllib3

from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
    SADIPEM_PVL_URL,
    TESOURO_CKAN_SEARCH_URL,
    StnTesouroTransparenteConnector,
)

urllib3.disable_warnings()


@pytest.mark.live
def test_stn_tesouro_live_smoke_connection() -> None:
    """Verify live connectivity against STN SADIPEM API and fail-hard on HTTP 5xx."""
    conn = StnTesouroTransparenteConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["sadipem_status"] == 200
    assert report["ckan_status"] == 200
    assert report["sample_pvl_id"] is not None


@pytest.mark.live
def test_stn_sadipem_live_pvl_endpoint() -> None:
    """Verify official STN SADIPEM API returns active credit operations (PVL)."""
    headers = {
        "User-Agent": "PublicDataGateway/1.0 (Government Integration; contact@publicdata.gateway)"
    }
    url = f"{SADIPEM_PVL_URL}?ano_solicitacao=2024"
    resp = requests.get(url, headers=headers, timeout=30, verify=False)
    assert resp.status_code == 200, f"STN SADIPEM returned HTTP {resp.status_code}"

    data = resp.json()
    assert "items" in data, "SADIPEM response must contain 'items' collection"
    items = data["items"]
    assert len(items) > 0, "STN SADIPEM must return at least one active pleito"

    sample = items[0]
    assert "id_pleito" in sample, "id_pleito must exist in SADIPEM record"
    assert "interessado" in sample, "interessado must exist in SADIPEM record"
    assert "valor" in sample, "valor must exist in SADIPEM record"


@pytest.mark.live
def test_stn_ckan_live_package_search() -> None:
    """Verify official Tesouro Transparente CKAN catalog API returns valid datasets."""
    headers = {
        "User-Agent": "PublicDataGateway/1.0 (Government Integration; contact@publicdata.gateway)"
    }
    url = f"{TESOURO_CKAN_SEARCH_URL}?q=transferencias&rows=2"
    resp = requests.get(url, headers=headers, timeout=30, verify=False)
    assert resp.status_code == 200, f"Tesouro CKAN returned HTTP {resp.status_code}"

    data = resp.json()
    assert data.get("success") is True, "Tesouro CKAN response must have success=true"
    results = data.get("result", {}).get("results", [])
    assert len(results) > 0, "Expected at least one dataset returned for 'transferencias'"
