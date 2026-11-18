"""Fail-hard live integration tests against ANS Open Data PDA repository and gov.br/ans."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.saude.ans_saude_suplementar import (
    ANS_DADOS_ABERTOS_URL,
    ANS_GOV_PORTAL_URL,
    AnsSaudeSuplementarConnector,
)


@pytest.mark.live
def test_ans_dados_abertos_pda_live_fail_hard() -> None:
    """Gate 11: Validate live ANS Dados Abertos PDA repository."""
    result = AnsSaudeSuplementarConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "ans.gov.br" in ANS_DADOS_ABERTOS_URL.lower()


@pytest.mark.live
def test_ans_gov_portal_live_fail_hard() -> None:
    """Gate 11: Validate live gov.br ANS institutional portal availability."""
    resp = httpx.get(
        ANS_GOV_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 10000
