"""Fail-hard live integration tests against BNDES Dados Abertos CKAN API and portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
    BNDES_CKAN_API_URL,
    BNDES_DADOS_ABERTOS_PORTAL_URL,
    BndesOperacoesConnector,
)


@pytest.mark.live
def test_bndes_ckan_api_live_fail_hard() -> None:
    """Gate 11: Validate live BNDES Dados Abertos CKAN REST API."""
    result = BndesOperacoesConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["packages_count"] >= 5
    assert result["content_length"] >= 500
    assert "package_list" in BNDES_CKAN_API_URL.lower()


@pytest.mark.live
def test_bndes_portal_live_fail_hard() -> None:
    """Gate 11: Validate live BNDES Dados Abertos portal availability."""
    resp = httpx.get(
        BNDES_DADOS_ABERTOS_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
