"""Live fail-hard tests against official Receita Federal CNO endpoints."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.empresarial.rfb_cno import (
    RFB_CNO_PORTAL_URL,
    RfbCnoConnector,
)


@pytest.mark.live
def test_rfb_cno_live_smoke_connection() -> None:
    """Verify live connectivity against RFB CNO official portal and fail hard on HTTP 5xx."""
    conn = RfbCnoConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["content_length"] > 1000


@pytest.mark.live
def test_rfb_cno_live_portal_get() -> None:
    """Verify official Receita Federal CNO page responds HTTP 200 with CNO documentation."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    resp = httpx.get(
        RFB_CNO_PORTAL_URL,
        headers=headers,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, f"RFB CNO returned HTTP {resp.status_code}"
    assert "cno" in resp.text.lower()
