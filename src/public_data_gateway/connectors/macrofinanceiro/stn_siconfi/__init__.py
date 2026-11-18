"""STN SICONFI connector package (Finanças Públicas Municipais & Estaduais)."""

from public_data_gateway.connectors.macrofinanceiro.stn_siconfi.connector import (
    SICONFI_BASE_URL,
    SICONFI_ENTES_URL,
    StnSiconfiConnector,
)

__all__ = [
    "SICONFI_BASE_URL",
    "SICONFI_ENTES_URL",
    "StnSiconfiConnector",
]
