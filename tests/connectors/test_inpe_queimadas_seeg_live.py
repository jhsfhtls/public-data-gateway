"""Live fail-hard tests against official INPE Queimadas and SEEG portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
    SEEG_PORTAL_URL,
    InpeQueimadasSeegConnector,
)


@pytest.mark.live
def test_inpe_queimadas_seeg_live_smoke_connection() -> None:
    """Verify live connectivity against INPE Queimadas COIDS daily CSV dataserver."""
    conn = InpeQueimadasSeegConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["content_length"] > 500


@pytest.mark.live
def test_inpe_queimadas_seeg_live_seeg_portal() -> None:
    """Verify Observatório do Clima SEEG portal responds HTTP 200 with SEEG markers."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    resp = httpx.get(
        SEEG_PORTAL_URL,
        headers=headers,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, f"SEEG portal returned HTTP {resp.status_code}"
    assert "seeg" in resp.text.lower()
