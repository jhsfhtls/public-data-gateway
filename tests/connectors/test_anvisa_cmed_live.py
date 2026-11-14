"""Live fail-hard integration test for ANVISA CMED Connector."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
    ANVISACmedConnector,
)


@pytest.mark.live
def test_anvisa_cmed_live_endpoints() -> None:
    """Valida conectividade real com endpoints oficiais ANVISA CMED."""
    connector = ANVISACmedConnector()
    report = connector.verify_live()
    assert report["source_id"] == "anvisa_cmed"
    assert report["status"] == "PASS", f"Falha live ANVISA CMED: {report}"
