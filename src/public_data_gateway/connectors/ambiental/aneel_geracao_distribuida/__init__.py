"""ANEEL Geracao Distribuida (MMGD — Solar, Bioenergia, Eolica & Rede) Medallion connector."""

from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida.connector import (
    ANEEL_CKAN_MMGD_PACKAGE_URL,
    ANEEL_MMGD_PORTAL_URL,
    AneelGeracaoDistribuidaConnector,
)

__all__ = [
    "ANEEL_CKAN_MMGD_PACKAGE_URL",
    "ANEEL_MMGD_PORTAL_URL",
    "AneelGeracaoDistribuidaConnector",
]
