"""Connectors package for Brazil Data Gateway.

Registers domain-specific data source connectors across demografia, macroeconomia,
energia, saude, transito, etc.
"""

# Ensure built-in connectors are imported and registered
import public_data_gateway.connectors.agregadores.brasilapi_core  # noqa: F401
import public_data_gateway.connectors.demografico.ibge_localidades  # noqa: F401
import public_data_gateway.connectors.empresarial.rfb_cnpj_lakehouse  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_olinda_ptax  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_sgs  # noqa: F401
