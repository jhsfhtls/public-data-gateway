"""Fail-hard live integration tests against SUSEP SES Statistics and institutional portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
    SUSEP_GOV_PORTAL_URL,
    SUSEP_SES_PORTAL_URL,
    SusepSegurosConnector,
)


@pytest.mark.live
def test_susep_ses_statistics_portal_live_fail_hard() -> None:
    """Gate 11: Validate live SUSEP SES Statistical System portal availability."""
    result = SusepSegurosConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 5000
    assert "susep.gov.br" in SUSEP_SES_PORTAL_URL.lower()


@pytest.mark.live
def test_susep_gov_portal_live_fail_hard() -> None:
    """Gate 11: Validate live institutional SUSEP Gov.br portal availability."""
    resp = httpx.get(
        SUSEP_GOV_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=25.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
