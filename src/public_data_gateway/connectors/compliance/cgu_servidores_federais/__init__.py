"""CGU / SIAPE Servidores Federais (Funcionalismo Público, Folha & Acúmulos) connector."""

from public_data_gateway.connectors.compliance.cgu_servidores_federais.connector import (
    CGU_SERVIDORES_PORTAL_URL,
    MGI_SERVIDOR_PORTAL_URL,
    CguServidoresFederaisConnector,
)

__all__ = [
    "CGU_SERVIDORES_PORTAL_URL",
    "MGI_SERVIDOR_PORTAL_URL",
    "CguServidoresFederaisConnector",
]
