"""PRF Dados Abertos (Sinistros Rodoviários & Corredores Logísticos) connector."""

from public_data_gateway.connectors.geografico.prf_acidentes.connector import (
    PRF_ACIDENTES_PORTAL_URL,
    PRF_PORTAL_URL,
    PrfAccidentsConnector,
)

__all__ = [
    "PRF_PORTAL_URL",
    "PRF_ACIDENTES_PORTAL_URL",
    "PrfAccidentsConnector",
]
