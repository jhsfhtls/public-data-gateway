"""SINESP/MJSP & FBSP (Seguranca Publica, Ocorrencias & Risco Patrimonial) Medallion connector."""

from public_data_gateway.connectors.compliance.sinesp_seguranca.connector import (
    FBSP_ANUARIO_URL,
    SINESP_VDE_PORTAL_URL,
    SinespSegurancaConnector,
)

__all__ = [
    "FBSP_ANUARIO_URL",
    "SINESP_VDE_PORTAL_URL",
    "SinespSegurancaConnector",
]
