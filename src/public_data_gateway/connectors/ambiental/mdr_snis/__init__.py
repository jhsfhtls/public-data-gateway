"""MDR/MCidades SNIS & SINISA (Saneamento Basico — Agua, Esgoto & Residuos) Medallion connector."""

from public_data_gateway.connectors.ambiental.mdr_snis.connector import (
    SINISA_PORTAL_URL,
    SNIS_PORTAL_URL,
    MdrSnisConnector,
)

__all__ = [
    "SINISA_PORTAL_URL",
    "SNIS_PORTAL_URL",
    "MdrSnisConnector",
]
