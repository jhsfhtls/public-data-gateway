"""Live fail-hard integration test for Capitais IPTU/ITBI Connector."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
    CapitaisIptuItbiConnector,
)


@pytest.mark.live
def test_capitais_iptu_itbi_live_endpoints() -> None:
    """Valida conectividade real com Dados Abertos e portal ITBI Fazenda SP."""
    connector = CapitaisIptuItbiConnector()
    report = connector.probe_live()
    assert report["source_id"] == "capitais_iptu_itbi"
    assert report["status"] == "PASS", f"Falha live Capitais IPTU/ITBI: {report}"
    assert report["endpoints"]["portal_dados_abertos_gov"]["status_code"] == 200
    assert report["endpoints"]["portal_itbi_fazenda_sp"]["status_code"] == 200
