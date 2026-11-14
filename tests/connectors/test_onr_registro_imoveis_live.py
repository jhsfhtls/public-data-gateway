"""Live fail-hard integration test for ONR Registro de Imóveis SREI Connector."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
    ONRRegistroImoveisConnector,
)


@pytest.mark.live
def test_onr_registro_imoveis_live_endpoints() -> None:
    """Valida conectividade real com os portais oficiais ONR e Mapa SREI."""
    connector = ONRRegistroImoveisConnector()
    report = connector.probe_live()
    assert report["source_id"] == "onr_registro_imoveis"
    assert report["status"] == "PASS", f"Falha live ONR SREI: {report}"
