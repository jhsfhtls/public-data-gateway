"""Receita Federal CAFIR / CIB / CNIR (Cadastro de Imóveis Rurais) Medallion connector."""

from public_data_gateway.connectors.ambiental.rfb_cafir.connector import (
    INCRA_CNIR_PORTAL_URL,
    RFB_CAFIR_ITR_PORTAL_URL,
    RfbCafirConnector,
)

__all__ = [
    "INCRA_CNIR_PORTAL_URL",
    "RFB_CAFIR_ITR_PORTAL_URL",
    "RfbCafirConnector",
]
