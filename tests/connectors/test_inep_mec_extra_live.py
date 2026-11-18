"""Fail-hard live integration tests against INEP Microdata, FNDE/FUNDEB, and CNPq portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.educacao.inep_mec_extra import (
    CNPQ_DADOS_ABERTOS_URL,
    FNDE_DADOS_ABERTOS_URL,
    INEP_MICRODADOS_URL,
    InepMecExtraConnector,
)


@pytest.mark.live
def test_inep_microdados_portal_live_fail_hard() -> None:
    """Gate 11: Validate live INEP Microdata portal availability."""
    result = InepMecExtraConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 5000
    assert "gov.br/inep" in INEP_MICRODADOS_URL.lower()


@pytest.mark.live
def test_fnde_and_cnpq_open_data_portals_live_fail_hard() -> None:
    """Gate 11: Validate live FNDE (FUNDEB) and CNPq Open Data portals availability."""
    headers = {"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"}
    for url in (FNDE_DADOS_ABERTOS_URL, CNPQ_DADOS_ABERTOS_URL):
        resp = httpx.get(
            url,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert len(resp.content) > 5000
