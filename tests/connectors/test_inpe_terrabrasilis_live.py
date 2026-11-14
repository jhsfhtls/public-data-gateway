"""Teste live para endpoints públicos do conector inpe_terrabrasilis."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
    InpeTerrabrasilisConnector,
)


@pytest.mark.live
def test_inpe_terrabrasilis_live_endpoints() -> None:
    conn = InpeTerrabrasilisConnector()
    probes = conn.probe_live_endpoints(timeout=12)
    assert probes["terrabrasilis_portal"] == 200
    assert probes["terrabrasilis_geoserver_wfs"] == 200
