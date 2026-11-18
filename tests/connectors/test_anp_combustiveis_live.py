"""Fail-hard live integration tests against ANP Dados Abertos official portals."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.anp_combustiveis import (
    ANP_PRECOS_SEMANAIS_URL,
    ANP_VENDAS_DERIVADOS_URL,
    AnpCombustiveisConnector,
)


@pytest.mark.live
def test_anp_combustiveis_live_precos_fail_hard() -> None:
    """Gate 11: Validate live ANP weekly fuel prices portal availability."""
    result = AnpCombustiveisConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["content_length"] >= 5000
    assert "anp" in ANP_PRECOS_SEMANAIS_URL.lower()


@pytest.mark.live
def test_anp_combustiveis_vendas_derivados_live_fail_hard() -> None:
    """Gate 11: Validate live ANP petroleum derivative and biofuel sales portal."""
    resp = httpx.get(
        ANP_VENDAS_DERIVADOS_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
