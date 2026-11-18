"""Fail-hard live integration tests against CGU Portal da Transparência & MDS portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.compliance.cgu_beneficios_cidadao import (
    CGU_BOLSA_FAMILIA_DOWNLOAD_URL,
    CGU_DADOS_ABERTOS_URL,
    MDS_DADOS_ABERTOS_URL,
    CguBeneficiosCidadaoConnector,
)


@pytest.mark.live
def test_cgu_novo_bolsa_familia_portal_live_fail_hard() -> None:
    """Gate 11: Validate live CGU Portal da Transparência Novo Bolsa Família endpoint."""
    result = CguBeneficiosCidadaoConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 5000
    assert "portaldatransparencia.gov.br" in CGU_BOLSA_FAMILIA_DOWNLOAD_URL.lower()


@pytest.mark.live
def test_cgu_and_mds_open_data_portals_live_fail_hard() -> None:
    """Gate 11: Validate live CGU and MDS Open Data portals availability."""
    headers = {"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"}
    for url in (CGU_DADOS_ABERTOS_URL, MDS_DADOS_ABERTOS_URL):
        resp = httpx.get(
            url,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert len(resp.content) > 5000
