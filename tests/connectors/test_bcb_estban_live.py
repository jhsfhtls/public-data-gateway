"""Live fail-hard tests against official BACEN ESTBAN portal endpoints."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
    BCB_ESTBAN_PORTAL_URL,
    BcbEstbanConnector,
)


@pytest.mark.live
def test_bcb_estban_live_smoke_connection() -> None:
    """Verify live connectivity against BACEN ESTBAN portal and fail hard on HTTP 5xx."""
    conn = BcbEstbanConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["content_length"] > 500


@pytest.mark.live
def test_bcb_estban_live_portal_get() -> None:
    """Verify official BACEN ESTBAN municipal banking statistics page responds HTTP 200."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    resp = httpx.get(
        BCB_ESTBAN_PORTAL_URL,
        headers=headers,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, f"BACEN ESTBAN returned HTTP {resp.status_code}"
    assert "bcb" in resp.text.lower()
