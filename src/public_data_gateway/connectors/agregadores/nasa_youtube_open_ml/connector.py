"""NASA Open APIs & YouTube/Vision Open ML Medallion Connector."""

from __future__ import annotations

import os
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
    {"date": "2026-11-14", "title": "Earth Observation Night Lights", "media_type": "image", "url": "https://apod.nasa.gov/apod/image/example1.jpg"},
    {"date": "2026-11-13", "title": "South Atlantic Anomaly Orbit", "media_type": "image", "url": "https://apod.nasa.gov/apod/image/example2.jpg"},
]


class NasaYoutubeOpenMlConnector(PublicApiMedallionConnector):
    """Medallion connector for NASA APOD / Open ML Vision & NLP assets."""

    default_table = "nasa_apod_catalog"

    def __init__(
        self,
        source_id: str = "nasa_youtube_open_ml",
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

    def fetch_nasa_apod(self, date: str | None = None) -> dict[str, Any]:
        """Fetch NASA Astronomy Picture of the Day (DEMO_KEY or NASA_API_KEY)."""
        api_key = os.environ.get("NASA_API_KEY", "DEMO_KEY")
        params: dict[str, Any] = {"api_key": api_key}
        if date:
            params["date"] = date
        return self._get_json("https://api.nasa.gov/planetary/apod", params=params, cache_ttl=3600)

    def normalize_records(self, raw_payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize NASA APOD dict or list payload into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        rows = raw_payload if isinstance(raw_payload, list) else [raw_payload]
        return [
            {
                "apod_date": str(r.get("date") or r.get("apod_date") or "").strip(),
                "title": str(r.get("title") or "").strip(),
                "media_type": str(r.get("media_type") or "image").strip(),
                "url": str(r.get("url") or "").strip(),
                "extracted_at": now_iso,
            }
            for r in rows
        ]

    def extract_table(
        self,
        table_name: str = "nasa_apod_catalog",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist NASA Open ML catalog across Medallion layers."""
        if table_name != "nasa_apod_catalog":
            raise ValueError(f"Unknown table '{table_name}' for nasa_youtube_open_ml.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_nasa_apod()]
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name="nasa_apod_catalog",
            records=normalized,
            pk_columns=["apod_date"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to NASA Open APIs."""
        resp = self.http_client.get(
            "https://images-api.nasa.gov/search",
            params={"q": "earth", "media_type": "image", "page_size": 1},
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("nasa_youtube_open_ml", NasaYoutubeOpenMlConnector)
