"""Teste live de conectividade com o portal oficial SFB/SICAR."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.ambiental.sfb_sicar import SfbSicarConnector


@pytest.mark.live
def test_sfb_sicar_live_connection() -> None:
    connector = SfbSicarConnector()
    result = connector.test_connection()
    assert result["ok"] is True
    assert result["sfb_status"] == 200
