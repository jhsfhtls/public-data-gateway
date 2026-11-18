"""STN CAPAG (Capacidade de Pagamento) & FIRJAN IFGF Medallion connector."""

from public_data_gateway.connectors.macrofinanceiro.stn_capag.connector import (
    STN_CAPAG_CKAN_URL,
    StnCapagConnector,
)

__all__ = ["STN_CAPAG_CKAN_URL", "StnCapagConnector"]
