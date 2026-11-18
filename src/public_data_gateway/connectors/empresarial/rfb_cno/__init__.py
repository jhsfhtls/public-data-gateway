"""Receita Federal do Brasil (RFB) Cadastro Nacional de Obras (CNO) Medallion connector."""

from public_data_gateway.connectors.empresarial.rfb_cno.connector import (
    RFB_CNO_PORTAL_URL,
    RfbCnoConnector,
)

__all__ = ["RFB_CNO_PORTAL_URL", "RfbCnoConnector"]
