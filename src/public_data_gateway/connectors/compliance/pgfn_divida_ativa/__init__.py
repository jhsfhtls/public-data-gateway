"""PGFN Dívida Ativa da União & FGTS connector package (Compliance & Tax Risk)."""

from public_data_gateway.connectors.compliance.pgfn_divida_ativa.connector import (
    PGFN_OPEN_DATA_URL,
    PgfnDividaAtivaConnector,
)

__all__ = [
    "PGFN_OPEN_DATA_URL",
    "PgfnDividaAtivaConnector",
]
