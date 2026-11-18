"""BACEN SICOR (Crédito Rural, Custeio, Investimento & Proagro) Medallion connector."""

from public_data_gateway.connectors.ambiental.bcb_sicor.connector import (
    BCB_SICOR_CUSTEIO_URL,
    BCB_SICOR_INVESTIMENTO_URL,
    BCB_SICOR_ODATA_ROOT_URL,
    BcbSicorConnector,
)

__all__ = [
    "BCB_SICOR_CUSTEIO_URL",
    "BCB_SICOR_INVESTIMENTO_URL",
    "BCB_SICOR_ODATA_ROOT_URL",
    "BcbSicorConnector",
]
