"""Fail-hard connectivity probe against official INEP Censo da Educação Superior portals."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
    INEPCensoSuperiorConnector,
)


@pytest.mark.live
def test_inep_censo_superior_verify_live() -> None:
    """Garante conectividade real com endpoints públicos do INEP Censo da Educação Superior."""
    report = INEPCensoSuperiorConnector().verify_live()
    assert report["source_id"] == "inep_censo_superior"
    assert report["status"] in {"OK", "PASS"}, report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report
