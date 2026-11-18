"""Fail-hard live integration tests against SINESP MJSP and FBSP Anuario portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.compliance.sinesp_seguranca import (
    FBSP_ANUARIO_URL,
    SINESP_VDE_PORTAL_URL,
    SinespSegurancaConnector,
)


@pytest.mark.live
def test_sinesp_seguranca_live_portal_fail_hard() -> None:
    """Gate 11: Validate live Ministry of Justice SINESP-VDE portal availability."""
    result = SinespSegurancaConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "sinesp" in SINESP_VDE_PORTAL_URL.lower()


@pytest.mark.live
def test_fbsp_anuario_seguranca_live_fail_hard() -> None:
    """Gate 11: Validate live Brazilian Public Security Forum (FBSP) Anuario portal."""
    resp = httpx.get(
        FBSP_ANUARIO_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
