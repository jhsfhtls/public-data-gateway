"""TSE Dados Abertos Eleitorais connector package."""

from public_data_gateway.connectors.compliance.tse_eleicoes.connector import (
    TSE_DADOS_ABERTOS_URL,
    TSE_DIVULGACANDCONTAS_API_URL,
    TseEleicoesConnector,
)

__all__ = [
    "TSE_DADOS_ABERTOS_URL",
    "TSE_DIVULGACANDCONTAS_API_URL",
    "TseEleicoesConnector",
]
