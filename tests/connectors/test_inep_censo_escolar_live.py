"""Fail-hard live connectivity probe against official INEP Censo Escolar portals."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
    INEPCensoEscolarConnector,
)


@pytest.mark.live
def test_inep_censo_escolar_live_probe() -> None:
    """Garante conectividade real com endpoints públicos oficiais do INEP Censo Escolar."""
    report = INEPCensoEscolarConnector().verify_live()
    assert report["source_id"] == "inep_censo_escolar"
    assert report["status"] == "OK", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report
