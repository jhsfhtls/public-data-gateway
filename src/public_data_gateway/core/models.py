"""Pydantic v2 data models for catalog sources, ODCS contracts, and observability."""

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SourceEntry(BaseModel):
    """Catalog metadata describing an official Brazilian data source."""

    model_config = ConfigDict(extra="ignore")

    source_id: str
    name: str
    agency: str
    official_url: str
    segment_id: str
    segment_name: str
    access_mode: str
    protocol: str
    auth_type: str = "none"
    canonical_keys: list[str] = Field(default_factory=list)
    update_frequency: str = "daily"
    estimated_volume: str = ""
    lgpd_tier: str = "public"
    ml_feature_domains: list[str] = Field(default_factory=list)
    core_products_enabled: list[str] = Field(default_factory=list)
    rice_score: float = 0.0
    roadmap_wave: int = 1
    status: str = "planned"


class QualityRule(BaseModel):
    """Specification for column or table-level data quality validation."""

    model_config = ConfigDict(extra="ignore")

    rule_type: str
    column: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    severity: str = "error"


class ColumnContract(BaseModel):
    """Schema contract for a single column."""

    model_config = ConfigDict(extra="ignore")

    name: str
    data_type: str
    required: bool = True
    nullable: bool = True
    max_null_pct: float = 1.0
    sentinel_values: list[Any] = Field(default_factory=list)
    description: str = ""


class DataContract(BaseModel):
    """ODCS v3.1 profile for dataset quality and schema governance."""

    model_config = ConfigDict(extra="ignore")

    contract_id: str
    source_id: str
    version: str = "1.0.0"
    grain: str = ""
    primary_keys: list[str] = Field(default_factory=list)
    reference_date_column: str | None = None
    lgpd_legal_basis: str = "legitimate_interest"
    min_expected_rows: int = 0
    max_volume_drop_pct: float = 50.0
    sentinel_values: list[Any] = Field(default_factory=list)
    columns: list[ColumnContract] = Field(default_factory=list)
    quality_rules: list[QualityRule] = Field(default_factory=list)


class ObservabilityReport(BaseModel):
    """Evaluation result across the 5 Pillars of Data Observability."""

    model_config = ConfigDict(extra="ignore")

    freshness_ok: bool = True
    volume_ok: bool = True
    schema_ok: bool = True
    distribution_ok: bool = True
    lineage_hash: str = ""
    passed: bool = True
    violations: list[str] = Field(default_factory=list)
    quarantine_path: str | None = None
    checked_at: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat()
    )


class SnapshotManifest(BaseModel):
    """Persistent metadata record accompanying medallion Parquet snapshots."""

    model_config = ConfigDict(extra="ignore")

    source_id: str
    layer: str
    row_count: int
    sha256: str
    reference_date_min: str | None = None
    reference_date_max: str | None = None
    extracted_at: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat()
    )
    stale_fallback: bool = False


class EnrichmentReport(BaseModel):
    """Execution summary and audit trail for an enrichment operation."""

    model_config = ConfigDict(extra="ignore")

    entity_type: str
    total_rows: int
    matched_rows: int
    match_rate: float
    invalid_keys_count: int = 0
    sources_applied: list[str] = Field(default_factory=list)
    output_path: str | None = None
    sha256: str | None = None
