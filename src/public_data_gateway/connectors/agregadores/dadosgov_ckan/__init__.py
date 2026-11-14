"""Pacote do conector Dados.gov.br: Portal Brasileiro de Dados Abertos (Catálogo CKAN)."""

from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
    DadosGovCkanConnector,
)

__all__ = ["DadosGovCkanConnector"]
