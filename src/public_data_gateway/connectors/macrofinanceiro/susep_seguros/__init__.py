"""SUSEP (Mercado Segurador, Previdência Complementar & Solvência) connector."""

from public_data_gateway.connectors.macrofinanceiro.susep_seguros.connector import (
    SUSEP_GOV_PORTAL_URL,
    SUSEP_SES_PORTAL_URL,
    SusepSegurosConnector,
)

__all__ = [
    "SUSEP_SES_PORTAL_URL",
    "SUSEP_GOV_PORTAL_URL",
    "SusepSegurosConnector",
]
