"""Fail-hard live integration tests against SEDEC Civil Defense and CEMADEN portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.sedec_desastres import (
    CEMADEN_PORTAL_URL,
    SEDEC_PORTAL_URL,
    SedecDesastresConnector,
)


@pytest.mark.live
def test_sedec_desastres_live_portal_fail_hard() -> None:
    """Gate 11: Validate live SEDEC/MIDR Civil Defense portal availability."""
    result = SedecDesastresConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "defesa-civil" in SEDEC_PORTAL_URL.lower()


@pytest.mark.live
def test_cemaden_portal_live_fail_hard() -> None:
    """Gate 11: Validate live CEMADEN/MCTI official monitoring portal availability."""
    resp = httpx.get(
        CEMADEN_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
