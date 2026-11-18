"""MPS / INSS (Previdência Social, Benefícios Emitidos & Arrecadação) connector."""

from public_data_gateway.connectors.trabalho.mps_inss_beneficios.connector import (
    INSS_CKAN_PORTAL_URL,
    MPS_PORTAL_URL,
    MpsInssBeneficiosConnector,
)

__all__ = [
    "INSS_CKAN_PORTAL_URL",
    "MPS_PORTAL_URL",
    "MpsInssBeneficiosConnector",
]
