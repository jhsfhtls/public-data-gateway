"""Connectors package for Brazil Data Gateway.

Registers domain-specific data source connectors across demografia, macroeconomia,
energia, saude, transito, etc.
"""

# Ensure built-in connectors are imported and registered
import brazil_data_gateway.connectors.demografico.ibge_localidades  # noqa: F401
import brazil_data_gateway.connectors.macrofinanceiro.bcb_sgs  # noqa: F401
