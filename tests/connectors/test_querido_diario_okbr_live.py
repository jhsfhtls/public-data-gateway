"""Fail-hard live integration tests against Querido Diário OKBR and ok.org.br portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
    OKBR_INSTITUTIONAL_URL,
    QUERIDO_DIARIO_PORTAL_URL,
    QueridoDiarioOkbrConnector,
)


@pytest.mark.live
def test_querido_diario_portal_live_fail_hard() -> None:
    """Gate 11: Validate live Querido Diário OKBR portal availability."""
    result = QueridoDiarioOkbrConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "queridodiario" in QUERIDO_DIARIO_PORTAL_URL.lower()


@pytest.mark.live
def test_okbr_institutional_portal_live_fail_hard() -> None:
    """Gate 11: Validate live Open Knowledge Brasil portal availability."""
    resp = httpx.get(
        OKBR_INSTITUTIONAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 10000
