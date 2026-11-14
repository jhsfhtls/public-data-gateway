"""Live fail-hard integration test for INEP ENEM Connector."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.educacao.inep_enem.connector import (
    INEPEnemConnector,
)


@pytest.mark.live
def test_inep_enem_live_endpoints() -> None:
    """Valida conectividade real com os portais oficiais de Microdados e ENEM no INEP."""
    connector = INEPEnemConnector()
    report = connector.probe_live()
    assert report["source_id"] == "inep_enem"
    assert report["status"] == "PASS", f"Falha live INEP ENEM: {report}"
    assert report["endpoints"]["inep_enem_microdados"]["status_code"] == 200
    assert report["endpoints"]["inep_enem_portal"]["status_code"] == 200
