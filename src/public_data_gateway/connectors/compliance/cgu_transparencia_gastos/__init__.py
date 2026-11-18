"""CGU Portal da Transparência (Gastos Diretos, Transferências & Convênios) connector."""

from public_data_gateway.connectors.compliance.cgu_transparencia_gastos.connector import (
    CGU_PORTAL_URL,
    CGU_TRANSPARENCIA_PORTAL_URL,
    CguTransparenciaGastosConnector,
)

__all__ = [
    "CGU_TRANSPARENCIA_PORTAL_URL",
    "CGU_PORTAL_URL",
    "CguTransparenciaGastosConnector",
]
