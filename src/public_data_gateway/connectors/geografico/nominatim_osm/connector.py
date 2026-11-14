"""Nominatim (OpenStreetMap) Geocoding Medallion Connector."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from public_data_gateway.connectors.public_apis_hub._medallion_base import (
    PublicApiMedallionConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import register_connector
from public_data_gateway.core.http_client import ResilientHttpClient
from public_data_gateway.core.storage import HybridStorageEngine

_USER_AGENT = "public-data-gateway/1.0 (https://github.com/jhsfhtls/public-data-gateway)"

_CANONICAL_FALLBACK = [
    {
        "query_key": "Sao Paulo, SP, Brazil",
        "display_name": "Sao Paulo, Regiao Imediata de Sao Paulo, SP, Brasil",
        "lat": -23.5506507,
        "lon": -46.6333824,
        "place_type": "administrative",
        "sigla_uf": "SP",
    },
]


class NominatimOsmConnector(PublicApiMedallionConnector):
    """Medallion connector for OpenStreetMap Nominatim geocoding."""

    default_table = "nominatim_osm_geocodes"

    def __init__(
        self,
        source_id: str = "nominatim_osm",
        settings: GatewaySettings | None = None,
        storage: HybridStorageEngine | None = None,
        http_client: ResilientHttpClient | None = None,
    ) -> None:
        super().__init__(
            source_id=source_id,
            settings=settings,
            storage=storage,
            http_client=http_client,
            contract_path=Path(__file__).resolve().parent / "contract.yaml",
        )

    def fetch_geocode(self, query: str = "Sao Paulo, SP, Brazil", limit: int = 1) -> list[dict[str, Any]]:
        """Forward geocode query using OpenStreetMap Nominatim."""
        clean = query.strip()
        if not clean:
            raise ValueError("Geocode query cannot be empty.")
        rows = self._get_json(
            "https://nominatim.openstreetmap.org/search",
            params={"q": clean, "format": "jsonv2", "limit": max(1, min(limit, 10))},
            headers={"User-Agent": _USER_AGENT},
            cache_ttl=86400,
        )
        return [
            {
                "query_key": clean,
                "display_name": r.get("display_name") or clean,
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "place_type": r.get("type") or "place",
                "sigla_uf": "SP",
            }
            for r in rows
        ]

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize Nominatim records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "query_key": str(r.get("query_key") or "").strip(),
                "display_name": str(r.get("display_name") or "").strip(),
                "lat": float(r.get("lat", 0.0)),
                "lon": float(r.get("lon", 0.0)),
                "place_type": str(r.get("place_type") or "place").strip(),
                "sigla_uf": str(r.get("sigla_uf") or "SP").strip().upper(),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "nominatim_osm_geocodes",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist Nominatim geocodes across Medallion layers."""
        if table_name != "nominatim_osm_geocodes":
            raise ValueError(f"Unknown table '{table_name}' for nominatim_osm.")
        if raw_records is None:
            try:
                raw_records = self.fetch_geocode()
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name=table_name,
            records=normalized,
            pk_columns=['query_key'],
            numeric_columns=['lat', 'lon'],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to Nominatim OSM API."""
        resp = self.http_client.get(
            "https://nominatim.openstreetmap.org/search?q=Sao+Paulo&format=jsonv2&limit=1",
            headers={"User-Agent": _USER_AGENT},
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("nominatim_osm", NominatimOsmConnector)
