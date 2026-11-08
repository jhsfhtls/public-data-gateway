"""Backward-compatibility shim for brazil_data_gateway.

Aliases all modules and classes from public_data_gateway to maintain
uninterrupted compatibility with existing code and subagents.
"""

from __future__ import annotations

import sys

import public_data_gateway
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.normalizer import CanonicalNormalizer, CheckDigitValidator
from public_data_gateway.core.storage import HybridStorageEngine
from public_data_gateway.enrichment.engine import EnrichmentEngine
from public_data_gateway.sdk import BrazilData, PublicData

# Redirect sys.modules
sys.modules["brazil_data_gateway"] = public_data_gateway

# Map subpackages
for submod in [
    "api",
    "cli",
    "connectors",
    "connectors.demografico",
    "connectors.demografico.ibge_localidades",
    "connectors.macrofinanceiro",
    "connectors.macrofinanceiro.bcb_sgs",
    "connectors.macrofinanceiro.bcb_olinda_ptax",
    "connectors.agregadores",
    "connectors.agregadores.brasilapi_core",
    "connectors.empresarial",
    "connectors.empresarial.rfb_cnpj_lakehouse",
    "core",
    "core.bigquery_guard",
    "core.catalog",
    "core.config",
    "core.connector",
    "core.contracts",
    "core.http_client",
    "core.models",
    "core.normalizer",
    "core.security",
    "core.storage",
    "enrichment",
    "enrichment.engine",
    "mcp",
    "mcp.server",
    "sdk",
]:
    try:
        mod_obj = __import__(f"public_data_gateway.{submod}", fromlist=["*"])
        sys.modules[f"brazil_data_gateway.{submod}"] = mod_obj
    except Exception:
        pass

__all__ = [
    "BrazilData",
    "PublicData",
    "CanonicalNormalizer",
    "CheckDigitValidator",
    "EnrichmentEngine",
    "GatewaySettings",
    "HybridStorageEngine",
]
