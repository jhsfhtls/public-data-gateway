"""Core medallion lakehouse, security, and normalization modules."""

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.models import (
    ColumnContract,
    DataContract,
    EnrichmentReport,
    ObservabilityReport,
    QualityRule,
    SnapshotManifest,
    SourceEntry,
)
from brazil_data_gateway.core.normalizer import (
    CanonicalNormalizer,
    CheckDigitValidator,
    MonetaryNormalizer,
)
from brazil_data_gateway.core.security import (
    safe_extract_zip,
    sanitize_csv_value,
    verify_magic_bytes,
    wrap_untrusted_gov_data,
)
from brazil_data_gateway.core.storage import HybridStorageEngine

__all__ = [
    "GatewaySettings",
    "SourceEntry",
    "ColumnContract",
    "DataContract",
    "QualityRule",
    "ObservabilityReport",
    "SnapshotManifest",
    "EnrichmentReport",
    "CheckDigitValidator",
    "CanonicalNormalizer",
    "MonetaryNormalizer",
    "verify_magic_bytes",
    "safe_extract_zip",
    "sanitize_csv_value",
    "wrap_untrusted_gov_data",
    "HybridStorageEngine",
]
