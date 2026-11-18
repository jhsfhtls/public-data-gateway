"""SEDEC/S2ID & CEMADEN (Desastres Naturais, Decretacoes & Clima Extremo) Medallion connector."""

from public_data_gateway.connectors.ambiental.sedec_desastres.connector import (
    CEMADEN_PORTAL_URL,
    SEDEC_PORTAL_URL,
    SedecDesastresConnector,
)

__all__ = [
    "CEMADEN_PORTAL_URL",
    "SEDEC_PORTAL_URL",
    "SedecDesastresConnector",
]
