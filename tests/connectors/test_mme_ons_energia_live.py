"""Fail-hard live integration tests against ONS CKAN Open Data API and MME portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.mme_ons_energia import (
    MME_PORTAL_URL,
    ONS_CKAN_PORTAL_URL,
    MmeOnsEnergiaConnector,
)


@pytest.mark.live
def test_mme_ons_ckan_live_fail_hard() -> None:
    """Gate 11: Validate live ONS CKAN Open Data package catalog."""
    result = MmeOnsEnergiaConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["dataset_count"] > 0
    assert "ons.org.br" in ONS_CKAN_PORTAL_URL.lower()


@pytest.mark.live
def test_mme_official_portal_live_fail_hard() -> None:
    """Gate 11: Validate live Ministry of Mines and Energy (MME) portal availability."""
    resp = httpx.get(
        MME_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
