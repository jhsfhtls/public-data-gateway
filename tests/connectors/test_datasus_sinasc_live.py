"""Live fail-hard integration test for DataSUS SINASC Connector."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
    DataSUSSinascConnector,
)


@pytest.mark.live
def test_datasus_sinasc_live_endpoints() -> None:
    """Valida conectividade real com endpoints oficiais do DataSUS SINASC."""
    connector = DataSUSSinascConnector()
    report = connector.verify_live()
    assert report["source_id"] == "datasus_sinasc"
    assert report["status"] == "PASS", f"Falha live DataSUS SINASC: {report}"
