"""Fail-hard live integration tests against PRF official open data portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.geografico.prf_acidentes import (
    PRF_ACIDENTES_PORTAL_URL,
    PRF_PORTAL_URL,
    PrfAccidentsConnector,
)


@pytest.mark.live
def test_prf_acidentes_live_portal_fail_hard() -> None:
    """Gate 11: Validate live PRF Open Data portal availability."""
    result = PrfAccidentsConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "prf" in PRF_PORTAL_URL.lower()


@pytest.mark.live
def test_prf_acidentes_dataset_page_live_fail_hard() -> None:
    """Gate 11: Validate live PRF highway accidents dataset portal availability."""
    resp = httpx.get(
        PRF_ACIDENTES_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
