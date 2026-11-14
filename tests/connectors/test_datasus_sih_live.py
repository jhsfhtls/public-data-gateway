"""Fail-hard connectivity probe against official DataSUS SIH/SUS portals."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.saude.datasus_sih.connector import (
    DataSUSSihConnector,
)


@pytest.mark.live
def test_datasus_sih_verify_live() -> None:
    """Garante conectividade real com endpoints públicos do DataSUS SIH/SUS."""
    report = DataSUSSihConnector().verify_live()
    assert report["source_id"] == "datasus_sih"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report
