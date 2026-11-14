"""Teste live de conectividade com os portais oficiais MAPA ZARC."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.ambiental.mapa_zarc import MapaZarcConnector


@pytest.mark.live
def test_mapa_zarc_live_connection() -> None:
    connector = MapaZarcConnector()
    result = connector.test_connection()
    assert result["ok"] is True
    assert result["zarc_status"] == 200 or result["dados_status"] == 200
