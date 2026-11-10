"""Dedicated, row-bounded access to IBGE municipal GDP (SIDRA table 5938)."""

from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
    IBGEPibMunicipalConnector,
)
from public_data_gateway.core.connector import register_connector

register_connector("ibge_pib_municipal", IBGEPibMunicipalConnector)

__all__ = ["IBGEPibMunicipalConnector"]
