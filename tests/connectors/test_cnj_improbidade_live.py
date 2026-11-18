"""Live fail-hard tests against the official CNJ CNCIAI Improbidade Administrativa portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.judicial.cnj_improbidade import (
    CNJ_CNCIAI_URL,
    CnjImprobidadeConnector,
)


@pytest.mark.live
def test_cnj_improbidade_live_smoke_connection() -> None:
    """Verify live connectivity against the CNJ CNCIAI portal and fail hard on HTTP 5xx."""
    conn = CnjImprobidadeConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["content_length"] > 1000


@pytest.mark.live
def test_cnj_improbidade_live_portal_get() -> None:
    """Verify official CNJ CNCIAI consultation endpoint responds HTTP 200 with form content."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
    }
    resp = httpx.get(
        CNJ_CNCIAI_URL,
        headers=headers,
        timeout=20.0,
        follow_redirects=True,
        verify=False,
    )
    assert resp.status_code == 200, f"CNJ CNCIAI returned HTTP {resp.status_code}"
    assert "improbidade" in resp.text.lower() or "cnj" in resp.text.lower()
