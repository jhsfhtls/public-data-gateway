"""Pacote do conector Datalake Público Federado & Catálogo Nacional SQL (DuckDB / BigQuery)."""

from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
    DatalakeFederadoBqConnector,
)

__all__ = ["DatalakeFederadoBqConnector"]
