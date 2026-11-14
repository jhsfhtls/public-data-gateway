"""Live fail-hard integration test for INEP IDEB Connector."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.educacao.inep_ideb.connector import (
    INEPIdebConnector,
)


@pytest.mark.live
def test_inep_ideb_live_endpoints() -> None:
    """Valida conectividade real com endpoints oficiais INEP IDEB."""
    connector = INEPIdebConnector()
    report = connector.verify_live()
    assert report["source_id"] == "inep_ideb"
    assert report["status"] == "PASS", f"Falha live INEP IDEB: {report}"
