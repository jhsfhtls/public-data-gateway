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

    def __post_init__(self) -> None:
        if self.data_dir is None:
            env_override = os.environ.get("BDG_DATA_DIR")
            if env_override:
                self.data_dir = Path(env_override).resolve()
            else:
                self.data_dir = (self.repo_root / "data").resolve()
        else:
            self.data_dir = Path(self.data_dir).resolve()

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
