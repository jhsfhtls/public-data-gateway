"""Fail-hard connectivity probe against official DataSUS SIM portals."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.saude.datasus_sim.connector import (
    DataSUSSimConnector,
)


@pytest.mark.live
def test_datasus_sim_verify_live() -> None:
    """Garante conectividade real com endpoints públicos do DataSUS SIM."""
    report = DataSUSSimConnector().verify_live()
    assert report["source_id"] == "datasus_sim"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report
