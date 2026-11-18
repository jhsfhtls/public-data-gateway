"""INEP/MEC Extra (SAEB, PROUNI, SISU, FUNDEB & CNPq) connector."""

from public_data_gateway.connectors.educacao.inep_mec_extra.connector import (
    CNPQ_DADOS_ABERTOS_URL,
    FNDE_DADOS_ABERTOS_URL,
    INEP_MICRODADOS_URL,
    InepMecExtraConnector,
)

__all__ = [
    "INEP_MICRODADOS_URL",
    "FNDE_DADOS_ABERTOS_URL",
    "CNPQ_DADOS_ABERTOS_URL",
    "InepMecExtraConnector",
]
