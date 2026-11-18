"""CVM Mercado de Capitais Extra (RCVM 160, Administradores & Agentes) connector package."""

from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra.connector import (
    CVM_CKAN_API_URL,
    CVM_DADOS_ABERTOS_PORTAL_URL,
    CvmMercadoExtraConnector,
)

__all__ = [
    "CVM_CKAN_API_URL",
    "CVM_DADOS_ABERTOS_PORTAL_URL",
    "CvmMercadoExtraConnector",
]
