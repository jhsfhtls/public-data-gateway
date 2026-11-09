"""Live integration test probe for IBGE CNEFE endpoints.

Validates connectivity and HTTP 200 responses against live official IBGE FTP
and Servicodados endpoints.
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.geografico.ibge_cnefe import (
    IBGECnefeConnector,
)


@pytest.mark.live
def test_ibge_cnefe_live_connection() -> None:
    """Validate live connectivity to official IBGE FTP and API endpoints."""
    connector = IBGECnefeConnector()
    report = connector.verify_live()

    assert report["source_id"] == "ibge_cnefe"
    assert "checked_at" in report
    assert "endpoints" in report

    # Verify IBGE FTP CNEFE endpoint
    ftp_info = report["endpoints"].get("ftp_cnefe")
    assert ftp_info is not None, "Missing ftp_cnefe endpoint in verification report"
    assert ftp_info.get("status_code") == 200
    assert ftp_info.get("healthy") is True

    # Verify IBGE Servicodados API
    api_info = report["endpoints"].get("ibge_servicodados")
    assert api_info is not None, "Missing ibge_servicodados endpoint in verification report"
    assert api_info.get("status_code") == 200
    assert api_info.get("healthy") is True

    assert report["status"] == "PASS"
