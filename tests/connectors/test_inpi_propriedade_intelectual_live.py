"""Fail-hard live integration tests against INPI Open Data and RPI portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
    INPI_DADOS_ABERTOS_URL,
    INPI_RPI_URL,
    InpiPropriedadeIntelectualConnector,
)


@pytest.mark.live
def test_inpi_dados_abertos_live_fail_hard() -> None:
    """Gate 11: Validate live INPI Open Data portal availability."""
    result = InpiPropriedadeIntelectualConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_bytes"] > 5000
    assert "gov.br/inpi" in INPI_DADOS_ABERTOS_URL.lower()


@pytest.mark.live
def test_inpi_rpi_portal_live_fail_hard() -> None:
    """Gate 11: Validate live INPI Revista da Propriedade Industrial (RPI) portal availability."""
    resp = httpx.get(
        INPI_RPI_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=25.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
