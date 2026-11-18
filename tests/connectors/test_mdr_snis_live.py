"""Fail-hard live integration tests against Ministry of Cities SNIS and SINISA portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.mdr_snis import (
    SINISA_PORTAL_URL,
    SNIS_PORTAL_URL,
    MdrSnisConnector,
)


@pytest.mark.live
def test_mdr_snis_live_portal_fail_hard() -> None:
    """Gate 11: Validate live Ministry of Cities SNIS official portal availability."""
    result = MdrSnisConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "snis" in SNIS_PORTAL_URL.lower()


@pytest.mark.live
def test_mdr_sinisa_portal_live_fail_hard() -> None:
    """Gate 11: Validate live Ministry of Cities SINISA portal availability."""
    resp = httpx.get(
        SINISA_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
