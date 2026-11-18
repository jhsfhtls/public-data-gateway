"""Fail-hard live integration tests against INSS CKAN Open Data API and MPS portal."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
    INSS_CKAN_PORTAL_URL,
    MPS_PORTAL_URL,
    MpsInssBeneficiosConnector,
)


@pytest.mark.live
def test_mps_inss_ckan_live_fail_hard() -> None:
    """Gate 11: Validate live INSS CKAN Open Data package catalog."""
    result = MpsInssBeneficiosConnector().verify_live()
    assert result["status"] == "LIVE_SUCCESS"
    assert result["http_status"] == 200
    assert result["dataset_count"] > 0
    assert "inss.gov.br" in INSS_CKAN_PORTAL_URL.lower()


@pytest.mark.live
def test_mps_previdencia_portal_live_fail_hard() -> None:
    """Gate 11: Validate live Ministry of Social Security (MPS) portal availability."""
    resp = httpx.get(
        MPS_PORTAL_URL,
        headers={"User-Agent": "Mozilla/5.0 PublicDataGateway/1.0"},
        timeout=20.0,
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert len(resp.content) > 5000
