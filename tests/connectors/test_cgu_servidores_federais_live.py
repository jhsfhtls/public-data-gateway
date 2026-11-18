"""Fail-hard live integration tests against CGU Servidores portal and MGI Servidor portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
    CGU_SERVIDORES_PORTAL_URL,
    MGI_SERVIDOR_PORTAL_URL,
    CguServidoresFederaisConnector,
)


@pytest.mark.live
def test_cgu_servidores_portal_live_fail_hard() -> None:
    """Gate 11: Validate live Portal da Transparência Servidores portal."""
    result = CguServidoresFederaisConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "servidores" in CGU_SERVIDORES_PORTAL_URL.lower()


@pytest.mark.live
def test_mgi_servidor_portal_live_fail_hard() -> None:
    """Gate 11: Validate live MGI Portal do Servidor (SIAPE) availability."""
    resp = httpx.get(
        MGI_SERVIDOR_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
