"""Live fail-hard tests against official RFB CAFIR / ITR & INCRA CNIR endpoints."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.rfb_cafir import (
    INCRA_CNIR_PORTAL_URL,
    RfbCafirConnector,
)


@pytest.mark.live
def test_rfb_cafir_live_smoke_connection() -> None:
    """Verify live connectivity against official RFB ITR/CAFIR portal."""
    conn = RfbCafirConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["content_length"] > 1000


@pytest.mark.live
def test_rfb_cafir_live_incra_cnir_portal() -> None:
    """Verify live INCRA CNIR governance portal responds HTTP 200 with CNIR documentation."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    resp = httpx.get(
        INCRA_CNIR_PORTAL_URL,
        headers=headers,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, f"INCRA CNIR returned HTTP {resp.status_code}"
    assert "cnir" in resp.text.lower() or "imóvel rural" in resp.text.lower()
