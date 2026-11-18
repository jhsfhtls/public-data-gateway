"""Fail-hard live integration tests against IBGE Pesquisas MUNIC, Agregados & Localidades APIs."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.demografico.ibge_institucional import (
    IBGE_AGREGADOS_INSTITUCIONAL_URL,
    IBGE_ESTADOS_API_URL,
    IBGE_MUNIC_PESQUISA_URL,
    IbgeInstitucionalConnector,
)


@pytest.mark.live
def test_ibge_munic_pesquisas_api_live_fail_hard() -> None:
    """Gate 11: Validate live IBGE Pesquisas MUNIC API availability."""
    result = IbgeInstitucionalConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 500
    assert "servicodados.ibge.gov.br" in IBGE_MUNIC_PESQUISA_URL.lower()


@pytest.mark.live
def test_ibge_agregados_and_localidades_apis_live_fail_hard() -> None:
    """Gate 11: Validate live IBGE Agregados (Assunto 70) and Estados Localidades APIs."""
    headers = {"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"}
    for url in (IBGE_AGREGADOS_INSTITUCIONAL_URL, IBGE_ESTADOS_API_URL):
        resp = httpx.get(
            url,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert len(resp.content) > 500
        assert isinstance(resp.json(), (list, dict))
