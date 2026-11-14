from __future__ import annotations

import pytest

from public_data_gateway.connectors.agregadores.dadosgov_ckan import DadosGovCkanConnector


@pytest.mark.live
def test_dadosgov_ckan_live_probe() -> None:
    conn = DadosGovCkanConnector()
    probe = conn.test_connection()
    assert probe["ok"] is True
    assert probe["portal_status"] == 200 or probe["catalog_status"] == 200
