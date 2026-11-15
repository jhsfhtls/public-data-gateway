"""Configuration and environment settings for Brazil Data Gateway."""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class GatewaySettings:
    """Core settings for data storage, paths, engine limits, and network clients."""

    repo_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[3]
    )
    data_dir: Path | None = None
    duckdb_memory_limit: str = "4GB"
    user_agent: str = "public-data-gateway/0.1.0 (+https://github.com/jhsfhtls/public-data-gateway)"
    cache_ttl_seconds: int = 86400
    bq_max_bytes_billed: int = 1073741824  # 1 GiB safety limit
    mirror_provider: str = field(
        default_factory=lambda: os.environ.get("BDG_MIRROR_PROVIDER", "local").lower()
    )
    mirror_bucket: str = field(
        default_factory=lambda: os.environ.get("BDG_MIRROR_BUCKET", "")
    )
    mirror_prefix: str = field(
        default_factory=lambda: os.environ.get("BDG_MIRROR_PREFIX", "public-data-gateway")
    )
    mirror_endpoint_url: str = field(
        default_factory=lambda: os.environ.get("BDG_MIRROR_ENDPOINT_URL", "")
    )
    mirror_region: str = field(
        default_factory=lambda: os.environ.get("BDG_MIRROR_REGION", "auto")
    )
    mirror_local_dir: Path | None = None
    warehouse_provider: str = field(
        default_factory=lambda: os.environ.get("BDG_WAREHOUSE_PROVIDER", "duckdb").lower()
    )
    warehouse_database: str = field(
        default_factory=lambda: os.environ.get("BDG_MOTHERDUCK_DATABASE", "public_data_gateway")
    )
    bq_dataset: str = field(
        default_factory=lambda: os.environ.get("BDG_BQ_DATASET", "public_data_gateway")
    )

    def __post_init__(self) -> None:
        if self.data_dir is None:
            env_override = os.environ.get("PDG_DATA_DIR") or os.environ.get("BDG_DATA_DIR")
            if env_override:
                self.data_dir = Path(env_override).resolve()
            else:
                self.data_dir = (self.repo_root / "data").resolve()
        else:
            self.data_dir = Path(self.data_dir).resolve()
        if self.mirror_local_dir is None:
            mirror_env = os.environ.get("BDG_MIRROR_LOCAL_DIR")
            if mirror_env:
                self.mirror_local_dir = Path(mirror_env).resolve()
            else:
                self.mirror_local_dir = (self.data_dir / "mirror").resolve()
        else:
            self.mirror_local_dir = Path(self.mirror_local_dir).resolve()

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def reference_dir(self) -> Path:
        return self.data_dir / "reference"

    @property
    def catalog_path(self) -> Path:
        return self.repo_root / "catalog" / "sources_registry.yaml"

    def get_source_dir(self, source_id: str) -> Path:
        """Resolve isolated root folder for a specific data source (e.g. data/ibge_localidades/)."""
        return self.data_dir / source_id

    def get_source_layer_dir(self, source_id: str, layer: str) -> Path:
        """Resolve isolated medallion layer folder for a specific data source."""
        normalized = layer.lower().strip()
        valid_layers = {"bronze", "silver", "gold", "quarantine"}
        if normalized not in valid_layers:
            raise ValueError(f"Unknown medallion layer: {layer}. Must be one of {sorted(valid_layers)}")
        p = self.data_dir / source_id / normalized
        p.mkdir(parents=True, exist_ok=True)
        return p

    def get_layer_dir(self, layer: str) -> Path:
        """Resolve directory path for a specific medallion layer."""
        normalized = layer.lower().strip()
        layer_map = {
            "bronze": self.bronze_dir,
            "silver": self.silver_dir,
            "gold": self.gold_dir,
            "quarantine": self.quarantine_dir,
        }
        if normalized not in layer_map:
            raise ValueError(f"Unknown medallion layer: {layer}. Must be one of {list(layer_map.keys())}")
        return layer_map[normalized]

    def ensure_directories(self) -> None:
        """Lazily initialize directory layout on disk."""
        for path in (
            self.bronze_dir,
            self.silver_dir,
            self.gold_dir,
            self.quarantine_dir,
            self.cache_dir,
            self.reference_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
