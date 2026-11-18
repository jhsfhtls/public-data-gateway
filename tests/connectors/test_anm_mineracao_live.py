"""Fail-hard live integration tests against ANM Open Data and CFEM portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.anm_mineracao import (
    ANM_ARRECADACAO_CFEM_URL,
    ANM_DADOS_ABERTOS_URL,
    AnmMineracaoConnector,
)


@pytest.mark.live
def test_anm_dados_abertos_live_fail_hard() -> None:
    """Gate 11: Validate live ANM Open Data portal availability."""
    result = AnmMineracaoConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 5000
    assert "gov.br/anm" in ANM_DADOS_ABERTOS_URL.lower()


@pytest.mark.live
def test_anm_cfem_arrecadacao_portal_live_fail_hard() -> None:
    """Gate 11: Validate live ANM CFEM Mineral Royalty Collection portal availability."""
    resp = httpx.get(
        ANM_ARRECADACAO_CFEM_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=25.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
