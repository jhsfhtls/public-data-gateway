"""Teste live para endpoints públicos do conector capes_sucupira."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
    CapesSucupiraConnector,
)


@pytest.mark.live
def test_capes_sucupira_live_endpoints() -> None:
    conn = CapesSucupiraConnector()
    probes = conn.probe_live_endpoints(timeout=12)
    assert probes["capes_ckan_api"] == 200
    assert probes["capes_dados_abertos_portal"] == 200
