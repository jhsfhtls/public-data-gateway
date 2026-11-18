"""BNDES Transparência Operações & Fomento Municipal connector package."""

from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes.connector import (
    BNDES_CKAN_API_URL,
    BNDES_DADOS_ABERTOS_PORTAL_URL,
    BndesOperacoesConnector,
)

__all__ = [
    "BNDES_CKAN_API_URL",
    "BNDES_DADOS_ABERTOS_PORTAL_URL",
    "BndesOperacoesConnector",
]
