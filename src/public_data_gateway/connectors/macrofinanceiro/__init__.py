"""Macrofinanceiro connectors package."""

from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata import (
    BcbIfdataConnector,
)
from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr import (
    CvmDfpItrConnector,
)
from public_data_gateway.connectors.macrofinanceiro.cvm_fundos import (
    CvmFundosConnector,
)
from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente import (
    StnTesouroTransparenteConnector,
)

__all__ = [
    "BcbIfdataConnector",
    "CvmDfpItrConnector",
    "CvmFundosConnector",
    "StnTesouroTransparenteConnector",
]
