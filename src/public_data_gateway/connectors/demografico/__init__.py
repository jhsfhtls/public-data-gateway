"""Demographic, territorial, and socioeconomic connectors."""

from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
    IBGECenso2022Connector,
)
from public_data_gateway.connectors.demografico.ibge_localidades.connector import (
    IBGELocalidadesConnector,
)
from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
    IBGESidraConnector,
)

__all__ = [
    "IBGECenso2022Connector",
    "IBGELocalidadesConnector",
    "IBGESidraConnector",
]
