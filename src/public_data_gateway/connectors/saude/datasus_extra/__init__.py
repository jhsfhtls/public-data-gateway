"""DATASUS Extra (SIA, SINAN, SISVAN & PNS) connector."""

from public_data_gateway.connectors.saude.datasus_extra.connector import (
    DATASUS_PORTAL_URL,
    MS_DADOS_ABERTOS_URL,
    OPENDATASUS_PORTAL_URL,
    DatasusExtraConnector,
)

__all__ = [
    "DATASUS_PORTAL_URL",
    "OPENDATASUS_PORTAL_URL",
    "MS_DADOS_ABERTOS_URL",
    "DatasusExtraConnector",
]
