"""Connectors package for Brazil Data Gateway.

Registers domain-specific data source connectors across demografia, macroeconomia,
energia, saude, transito, etc.
"""

# Ensure built-in connectors are imported and registered
import public_data_gateway.connectors.agregadores.brasilapi_core  # noqa: F401
import public_data_gateway.connectors.comercio_exterior.mdic_comexstat  # noqa: F401
import public_data_gateway.connectors.compliance.cgu_ceis_cnep  # noqa: F401
import public_data_gateway.connectors.compliance.cgu_pep  # noqa: F401
import public_data_gateway.connectors.compras_publicas.pncp_contratacoes  # noqa: F401
import public_data_gateway.connectors.demografico.ibge_localidades  # noqa: F401
import public_data_gateway.connectors.empresarial.rfb_cnpj_lakehouse  # noqa: F401
import public_data_gateway.connectors.empresarial.rfb_simples_mei  # noqa: F401
import public_data_gateway.connectors.empresarial.suframa_pim_zfm  # noqa: F401
import public_data_gateway.connectors.judicial.cnj_datajud  # noqa: F401
import public_data_gateway.connectors.judicial.tst_cndt  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_ifdata  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_olinda_ptax  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_sgs  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.cvm_fundos  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente  # noqa: F401
