"""INPI (Patentes, Marcas, Desenhos Industriais & Software) connector."""

from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual.connector import (
    INPI_DADOS_ABERTOS_URL,
    INPI_DATALAKE_PORTAL_URL,
    INPI_RPI_URL,
    InpiPropriedadeIntelectualConnector,
)

__all__ = [
    "INPI_DADOS_ABERTOS_URL",
    "INPI_RPI_URL",
    "INPI_DATALAKE_PORTAL_URL",
    "InpiPropriedadeIntelectualConnector",
]
