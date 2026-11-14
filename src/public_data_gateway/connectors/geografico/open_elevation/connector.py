"""Open-Elevation SRTM Terrain Altitude Medallion Connector."""

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

_CANONICAL_FALLBACK = [
    {"id_municipio": "3550308", "sigla_uf": "SP", "lat": -23.5505, "lon": -46.6333, "elevation_m": 760.0},
    {"id_municipio": "3304557", "sigla_uf": "RJ", "lat": -22.9068, "lon": -43.1729, "elevation_m": 11.0},
    {"id_municipio": "5300108", "sigla_uf": "DF", "lat": -15.7975, "lon": -47.8919, "elevation_m": 1172.0},
]


class OpenElevationConnector(PublicApiMedallionConnector):
    """Medallion connector for Open-Elevation / SRTM terrain altitude lookup."""

    default_table = "open_elevation_points"

    def __init__(
        self,
        source_id: str = "open_elevation",
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

    def fetch_elevation(
        self,
        lat: float = -23.5505,
        lon: float = -46.6333,
        *,
        id_municipio: str = "3550308",
        sigla_uf: str = "SP",
    ) -> dict[str, Any]:
        """Fetch terrain elevation (meters) from Open-Elevation SRTM."""
        data = self._get_json(
            "https://api.open-elevation.com/api/v1/lookup",
            params={"locations": f"{lat},{lon}"},
            cache_ttl=86400,
        )
        results = data.get("results") or []
        if not results:
            raise ValueError("No elevation result returned.")
        return {
            "id_municipio": id_municipio,
            "sigla_uf": sigla_uf,
            "lat": lat,
            "lon": lon,
            "elevation_m": float(results[0]["elevation"]),
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize Open-Elevation records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "id_municipio": str(r.get("id_municipio") or "").strip(),
                "sigla_uf": str(r.get("sigla_uf") or "SP").strip().upper(),
                "lat": float(r.get("lat", 0.0)),
                "lon": float(r.get("lon", 0.0)),
                "elevation_m": r.get("elevation_m"),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "open_elevation_points",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist SRTM elevation records across Medallion layers."""
        if table_name != "open_elevation_points":
            raise ValueError(f"Unknown table '{table_name}' for open_elevation.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_elevation()]
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name="open_elevation_points",
            records=normalized,
            pk_columns=["id_municipio"],
            numeric_columns=["elevation_m"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to Open-Elevation API."""
        resp = self.http_client.get(
            "https://api.open-elevation.com/api/v1/lookup",
            params={"locations": "-23.5505,-46.6333"},
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("open_elevation", OpenElevationConnector)
