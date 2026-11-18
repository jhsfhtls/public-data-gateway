"""Fail-hard live integration tests against CGU Portal da Transparência and CGU portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
    CGU_PORTAL_URL,
    CGU_TRANSPARENCIA_PORTAL_URL,
    CguTransparenciaGastosConnector,
)


@pytest.mark.live
def test_cgu_transparencia_despesas_live_fail_hard() -> None:
    """Gate 11: Validate live Portal da Transparência federal expenses portal."""
    result = CguTransparenciaGastosConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "portaldatransparencia" in CGU_TRANSPARENCIA_PORTAL_URL.lower()


@pytest.mark.live
def test_cgu_official_portal_live_fail_hard() -> None:
    """Gate 11: Validate live CGU (Controladoria-Geral da União) official portal."""
    resp = httpx.get(
        CGU_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
