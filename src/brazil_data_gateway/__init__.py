"""Brazil Data Gateway: Unified Medallion Lakehouse, Enrichment Engine & Interfaces."""

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.normalizer import CanonicalNormalizer, CheckDigitValidator
from brazil_data_gateway.core.storage import HybridStorageEngine
from brazil_data_gateway.enrichment.engine import EnrichmentEngine
from brazil_data_gateway.sdk import BrazilData

__version__ = "0.1.0"

__all__ = [
    "BrazilData",
    "CanonicalNormalizer",
    "CheckDigitValidator",
    "HybridStorageEngine",
    "EnrichmentEngine",
    "GatewaySettings",
]
