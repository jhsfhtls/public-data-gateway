"""Public Procurement & Government Tenders (PNCP, Compras.gov) Connectors."""

from .comprasgov_catmat import ComprasGovCatmatConnector
from .pncp_contratacoes import PNCPContratacoesConnector

# Convenience aliases
PncpContratacoesConnector = PNCPContratacoesConnector

__all__ = ["ComprasGovCatmatConnector", "PNCPContratacoesConnector", "PncpContratacoesConnector"]
