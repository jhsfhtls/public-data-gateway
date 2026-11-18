"""MME / ONS (Sistema Interligado Nacional, Geração & Segurança Energética) connector."""

from public_data_gateway.connectors.ambiental.mme_ons_energia.connector import (
    MME_PORTAL_URL,
    ONS_CKAN_PORTAL_URL,
    MmeOnsEnergiaConnector,
)

__all__ = [
    "ONS_CKAN_PORTAL_URL",
    "MME_PORTAL_URL",
    "MmeOnsEnergiaConnector",
]
