"""Live fail-hard integration test for OpenDataSUS Vigilância Epidemiológica Connector."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
    OpenDataSUSVigilanciaConnector,
)


@pytest.mark.live
def test_opendatasus_vigilancia_live_endpoints() -> None:
    """Valida conectividade real com os portais oficiais OpenDataSUS e SVSA/MS."""
    connector = OpenDataSUSVigilanciaConnector()
    report = connector.probe_live()
    assert report["source_id"] == "opendatasus_vigilancia"
    assert report["status"] == "PASS", f"Falha live OpenDataSUS Vigilância: {report}"
    assert report["endpoints"]["opendatasus_portal"]["status_code"] == 200
    assert report["endpoints"]["svsa_ministerio_saude"]["status_code"] == 200
