"""Teste live de conectividade com os endpoints públicos da ANA (HidroWeb & Telemetria)."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria import (
    AnaHidrowebConnector,
)


@pytest.mark.live
def test_ana_hidroweb_live_connection() -> None:
    connector = AnaHidrowebConnector()
    result = connector.test_connection()
    assert result["ok"] is True
    assert result["portal_status"] == 200 or result["telemetria_status"] == 200
