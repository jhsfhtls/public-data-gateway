"""INPE Queimadas & SEEG (Focos de Calor & Emissões GEE) Medallion connector."""

from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg.connector import (
    INPE_QUEIMADAS_DATASERV_URL,
    SEEG_PORTAL_URL,
    InpeQueimadasSeegConnector,
)

__all__ = [
    "INPE_QUEIMADAS_DATASERV_URL",
    "SEEG_PORTAL_URL",
    "InpeQueimadasSeegConnector",
]
