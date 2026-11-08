"""Catalog registry loader and query interface for official Brazilian data sources."""

from pathlib import Path

import yaml

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.models import SourceEntry


class CatalogRegistry:
    """Loads and queries metadata catalog entries from catalog/sources_registry.yaml."""

    def __init__(self, settings: GatewaySettings | None = None, custom_catalog_path: Path | None = None) -> None:
        self.settings = settings or GatewaySettings()
        self.catalog_path = custom_catalog_path or self.settings.catalog_path
        self._sources: dict[str, SourceEntry] = {}
        self._load()

    def _load(self) -> None:
        """Parse sources registry from disk if available."""
        if not self.catalog_path.is_file():
            # If not yet created or running in isolated test mode
            return

        with open(self.catalog_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Expecting either list of sources or dict with 'sources' key
        items = data.get("sources", []) if isinstance(data, dict) else data
        if not isinstance(items, list):
            return

        for item in items:
            if isinstance(item, dict):
                try:
                    entry = SourceEntry.model_validate(item)
                    self._sources[entry.source_id] = entry
                except Exception:
                    continue

    def add_source(self, source: SourceEntry) -> None:
        """Dynamically add or override a source entry in the registry."""
        self._sources[source.source_id] = source

    def get_source(self, source_id: str) -> SourceEntry | None:
        """Retrieve a specific source by its identifier."""
        return self._sources.get(source_id)

    def list_sources(
        self,
        wave: int | None = None,
        segment_id: str | None = None,
        status: str | None = None,
        product_id: str | None = None,
    ) -> list[SourceEntry]:
        """Query sources with filtering, ordered by roadmap_wave (asc) and rice_score (desc)."""
        filtered = list(self._sources.values())

        if wave is not None:
            filtered = [s for s in filtered if s.roadmap_wave == wave]
        if segment_id is not None:
            filtered = [s for s in filtered if s.segment_id.lower() == segment_id.lower()]
        if status is not None:
            filtered = [s for s in filtered if s.status.lower() == status.lower()]
        if product_id is not None:
            filtered = [s for s in filtered if product_id in s.core_products_enabled]

        # Sort: roadmap_wave ascending, rice_score descending
        filtered.sort(key=lambda s: (s.roadmap_wave, -s.rice_score))
        return filtered
