"""Fail-hard live integration tests against SENATRAN official vehicle fleet portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.geografico.senatran_frota import (
    SENATRAN_FROTA_2025_URL,
    SENATRAN_PORTAL_URL,
    SenatranFrotaConnector,
)


@pytest.mark.live
def test_senatran_frota_live_portal_fail_hard() -> None:
    """Gate 11: Validate live SENATRAN vehicle fleet statistics portal availability."""
    result = SenatranFrotaConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "senatran" in SENATRAN_PORTAL_URL.lower()


@pytest.mark.live
def test_senatran_frota_2025_portal_live_fail_hard() -> None:
    """Gate 11: Validate live SENATRAN 2025 fleet dataset landing page."""
    resp = httpx.get(
        SENATRAN_FROTA_2025_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
