"""CGU Benefícios ao Cidadão (Bolsa Família, BPC, Seguro-Defeso & Garantia-Safra) connector."""

from public_data_gateway.connectors.compliance.cgu_beneficios_cidadao.connector import (
    CGU_BOLSA_FAMILIA_DOWNLOAD_URL,
    CGU_DADOS_ABERTOS_URL,
    MDS_DADOS_ABERTOS_URL,
    CguBeneficiosCidadaoConnector,
)

__all__ = [
    "CGU_BOLSA_FAMILIA_DOWNLOAD_URL",
    "CGU_DADOS_ABERTOS_URL",
    "MDS_DADOS_ABERTOS_URL",
    "CguBeneficiosCidadaoConnector",
]
