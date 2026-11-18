"""Fail-hard live integration tests against TSE DivulgaCandContas API and Dados Abertos TSE."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.compliance.tse_eleicoes import (
    TSE_DADOS_ABERTOS_URL,
    TSE_DIVULGACANDCONTAS_API_URL,
    TseEleicoesConnector,
)


@pytest.mark.live
def test_tse_divulgacandcontas_api_live_fail_hard() -> None:
    """Gate 11: Validate live TSE DivulgaCandContas REST API."""
    result = TseEleicoesConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["elections_count"] >= 1
    assert result["content_length"] >= 500
    assert "eleicao" in TSE_DIVULGACANDCONTAS_API_URL.lower()


@pytest.mark.live
def test_tse_dados_abertos_portal_live_fail_hard() -> None:
    """Gate 11: Validate live TSE Dados Abertos portal availability."""
    resp = httpx.get(
        TSE_DADOS_ABERTOS_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
