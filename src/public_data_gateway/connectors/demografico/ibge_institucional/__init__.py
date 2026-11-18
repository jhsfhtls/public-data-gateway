"""IBGE Institucional & Social (MUNIC, ESTADIC & POF) connector."""

from public_data_gateway.connectors.demografico.ibge_institucional.connector import (
    IBGE_AGREGADOS_INSTITUCIONAL_URL,
    IBGE_ESTADOS_API_URL,
    IBGE_MUNIC_PESQUISA_URL,
    IbgeInstitucionalConnector,
)

__all__ = [
    "IBGE_MUNIC_PESQUISA_URL",
    "IBGE_AGREGADOS_INSTITUCIONAL_URL",
    "IBGE_ESTADOS_API_URL",
    "IbgeInstitucionalConnector",
]
