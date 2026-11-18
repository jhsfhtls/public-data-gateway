"""RFB Arrecadação Tributária & Fiscal (IRPJ, CSLL, IPI, PIS/COFINS & Dívida Ativa PGFN) connector."""

from public_data_gateway.connectors.macrofinanceiro.rfb_arrecadacao.connector import (
    FAZENDA_DADOS_ABERTOS_URL,
    RFB_ARRECADACAO_UF_URL,
    RFB_DADOS_ABERTOS_URL,
    RfbArrecadacaoConnector,
)

__all__ = [
    "RFB_ARRECADACAO_UF_URL",
    "RFB_DADOS_ABERTOS_URL",
    "FAZENDA_DADOS_ABERTOS_URL",
    "RfbArrecadacaoConnector",
]
