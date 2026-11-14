"""Teste live de conectividade com o portal oficial Base dos Dados."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.agregadores.basedosdados_bq import (
    BaseDosDadosBqConnector,
)


@pytest.mark.live
def test_basedosdados_live_connection() -> None:
    connector = BaseDosDadosBqConnector()
    result = connector.test_connection()
    assert result["ok"] is True
    assert result["portal_status"] == 200
