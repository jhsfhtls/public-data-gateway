"""Fail-hard connectivity probe against official ANTT Dados Abertos portals."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
    AnttMalhaTransporteConnector,
)


@pytest.mark.live
def test_antt_malha_transporte_verify_live() -> None:
    """Garante conectividade real com endpoints públicos da ANTT."""
    report = AnttMalhaTransporteConnector().verify_live()
    assert report["source_id"] == "antt_malha_transporte"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report
