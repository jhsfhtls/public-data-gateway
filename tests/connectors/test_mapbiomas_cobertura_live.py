"""Teste live de conectividade com os endpoints públicos do MapBiomas."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.ambiental.mapbiomas_cobertura import (
    MapBiomasCoberturaConnector,
)


@pytest.mark.live
def test_mapbiomas_live_connection() -> None:
    connector = MapBiomasCoberturaConnector()
    result = connector.test_connection()
    assert result["ok"] is True
    assert result["portal_status"] == 200 or result["platform_status"] == 200
