"""Teste live de conectividade com os catálogos públicos federados nacionais e contratos ODCS."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.agregadores.datalake_federado_bq import (
    DatalakeFederadoBqConnector,
)


@pytest.mark.live
def test_datalake_federado_live_connection() -> None:
    connector = DatalakeFederadoBqConnector()
    result = connector.test_connection()
    assert result["ok"] is True
    assert result["portal_status"] == 200 or result["catalog_status"] == 200
    assert result["odcs_columns_verified"] >= 10
