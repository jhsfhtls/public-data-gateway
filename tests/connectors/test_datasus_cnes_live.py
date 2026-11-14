"""Fail-hard connectivity probe against official DataSUS CNES portals and open APIs."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.saude.datasus_cnes.connector import (
    DataSUSCnesConnector,
)


@pytest.mark.live
def test_datasus_cnes_verify_live() -> None:
    """Garante conectividade real com endpoints públicos do DataSUS CNES."""
    report = DataSUSCnesConnector().verify_live()
    assert report["source_id"] == "datasus_cnes"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report
