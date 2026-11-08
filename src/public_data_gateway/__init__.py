"""Public Data Gateway: Unified Medallion Lakehouse, Enrichment Engine & Interfaces."""

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.normalizer import CanonicalNormalizer, CheckDigitValidator
from public_data_gateway.core.storage import HybridStorageEngine
from public_data_gateway.enrichment.engine import EnrichmentEngine
from public_data_gateway.sdk import BrazilData, PublicData

__version__ = "0.1.0"

__all__ = [
    "BrazilData",
    "PublicData",
    "CanonicalNormalizer",
    "CheckDigitValidator",
    "HybridStorageEngine",
    "EnrichmentEngine",
    "GatewaySettings",
]
