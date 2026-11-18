"""ANP Combustiveis (LPC Precos, Margens, Postos & Biocombustiveis) Medallion connector."""

from public_data_gateway.connectors.ambiental.anp_combustiveis.connector import (
    ANP_PRECOS_SEMANAIS_URL,
    ANP_VENDAS_DERIVADOS_URL,
    AnpCombustiveisConnector,
)

__all__ = [
    "ANP_PRECOS_SEMANAIS_URL",
    "ANP_VENDAS_DERIVADOS_URL",
    "AnpCombustiveisConnector",
]
