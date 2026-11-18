"""Fail-hard live integration tests against ANEEL Dados Abertos CKAN API."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
    ANEEL_CKAN_MMGD_PACKAGE_URL,
    ANEEL_MMGD_PORTAL_URL,
    AneelGeracaoDistribuidaConnector,
)


@pytest.mark.live
def test_aneel_mmgd_live_ckan_api_fail_hard() -> None:
    """Gate 11: Validate live ANEEL Dados Abertos CKAN package_show endpoint."""
    result = AneelGeracaoDistribuidaConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["package_name"] == "relacao-de-empreendimentos-de-geracao-distribuida"
    assert result["resources_count"] >= 1


@pytest.mark.live
def test_aneel_mmgd_portal_live_fail_hard() -> None:
    """Gate 11: Validate live ANEEL MMGD dataset portal availability."""
    resp = httpx.get(ANEEL_MMGD_PORTAL_URL, timeout=20.0, follow_redirects=True)
    assert resp.status_code == 200
    assert len(resp.content) > 500
    assert "relacao-de-empreendimentos-de-geracao-distribuida" in ANEEL_CKAN_MMGD_PACKAGE_URL
