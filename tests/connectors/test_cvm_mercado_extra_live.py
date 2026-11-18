"""Fail-hard live integration tests against CVM Dados Abertos CKAN API and portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
    CVM_CKAN_API_URL,
    CVM_DADOS_ABERTOS_PORTAL_URL,
    CvmMercadoExtraConnector,
)


@pytest.mark.live
def test_cvm_ckan_api_live_fail_hard() -> None:
    """Gate 11: Validate live CVM Dados Abertos CKAN REST API."""
    result = CvmMercadoExtraConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["datasets_count"] >= 5
    assert result["content_length"] >= 500
    assert "package_list" in CVM_CKAN_API_URL.lower()


@pytest.mark.live
def test_cvm_dados_abertos_portal_live_fail_hard() -> None:
    """Gate 11: Validate live CVM Dados Abertos portal availability."""
    resp = httpx.get(
        CVM_DADOS_ABERTOS_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
