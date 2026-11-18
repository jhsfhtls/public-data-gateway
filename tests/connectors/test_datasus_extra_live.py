"""Fail-hard live integration tests against DATASUS and OpenDataSUS/MS portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.saude.datasus_extra import (
    DATASUS_PORTAL_URL,
    OPENDATASUS_PORTAL_URL,
    DatasusExtraConnector,
)


@pytest.mark.live
def test_datasus_portal_live_fail_hard() -> None:
    """Gate 11: Validate live DATASUS official portal availability."""
    result = DatasusExtraConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 5000
    assert "datasus.saude.gov.br" in DATASUS_PORTAL_URL.lower()


@pytest.mark.live
def test_opendatasus_dados_abertos_live_fail_hard() -> None:
    """Gate 11: Validate live Ministry of Health Open Data portal availability."""
    resp = httpx.get(
        OPENDATASUS_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=25.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
