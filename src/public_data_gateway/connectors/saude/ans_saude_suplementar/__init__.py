"""ANS Saúde Suplementar (Beneficiários, Operadoras, IDSS & Sinistralidade) connector package."""

from public_data_gateway.connectors.saude.ans_saude_suplementar.connector import (
    ANS_DADOS_ABERTOS_URL,
    ANS_GOV_PORTAL_URL,
    AnsSaudeSuplementarConnector,
)

__all__ = [
    "ANS_DADOS_ABERTOS_URL",
    "ANS_GOV_PORTAL_URL",
    "AnsSaudeSuplementarConnector",
]
