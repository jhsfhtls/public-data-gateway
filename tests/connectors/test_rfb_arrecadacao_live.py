"""Fail-hard live integration tests against Receita Federal & Ministério da Fazenda portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.rfb_arrecadacao import (
    FAZENDA_DADOS_ABERTOS_URL,
    RFB_ARRECADACAO_UF_URL,
    RFB_DADOS_ABERTOS_URL,
    RfbArrecadacaoConnector,
)


@pytest.mark.live
def test_rfb_arrecadacao_portal_live_fail_hard() -> None:
    """Gate 11: Validate live RFB Arrecadação por Estado portal availability."""
    result = RfbArrecadacaoConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 5000
    assert "receitafederal" in RFB_ARRECADACAO_UF_URL.lower()


@pytest.mark.live
def test_rfb_and_fazenda_open_data_portals_live_fail_hard() -> None:
    """Gate 11: Validate live RFB Dados Abertos and Ministério da Fazenda Open Data portals."""
    headers = {"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"}
    for url in (RFB_DADOS_ABERTOS_URL, FAZENDA_DADOS_ABERTOS_URL):
        resp = httpx.get(
            url,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert len(resp.content) > 5000
