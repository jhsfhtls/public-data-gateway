"""IBAMA SIFISC (Autos de Infração e Termos de Embargo) Medallion connector."""

from public_data_gateway.connectors.ambiental.ibama_fiscalizacao.connector import (
    IBAMA_CKAN_AUTOS_URL,
    IBAMA_CKAN_EMBARGOS_URL,
    IbamaFiscalizacaoConnector,
)

__all__ = [
    "IBAMA_CKAN_AUTOS_URL",
    "IBAMA_CKAN_EMBARGOS_URL",
    "IbamaFiscalizacaoConnector",
]
