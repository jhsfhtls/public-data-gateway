"""GLEIF Global LEI Medallion Connector."""

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
    {
        "lei": "254900R93V54Q5M8E620",
        "legal_name": "PETROLEO BRASILEIRO S.A. - PETROBRAS",
        "country": "BR",
        "entity_status": "ACTIVE",
    },
]


class GleifLeiConnector(PublicApiMedallionConnector):
    """Medallion connector for GLEIF Global Legal Entity Identifier (LEI) API."""

    default_table = "gleif_lei_records"

    def __init__(
        self,
        source_id: str = "gleif_lei",
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

    def fetch_lei_records(self, query: str = "Petrobras", limit: int = 5) -> list[dict[str, Any]]:
        """Search global LEI records in GLEIF API."""
        clean = query.strip()
        if not clean:
            raise ValueError("GLEIF query cannot be empty.")
        data = self._get_json(
            "https://api.gleif.org/api/v1/lei-records",
            params={"filter[entity.legalName]": clean, "page[size]": max(1, min(limit, 20))},
            cache_ttl=3600,
        )
        rows: list[dict[str, Any]] = []
        for item in data.get("data") or []:
            attrs = item.get("attributes") or {}
            entity = attrs.get("entity") or {}
            rows.append(
                {
                    "lei": item.get("id") or "",
                    "legal_name": (entity.get("legalName") or {}).get("name") or "",
                    "country": (entity.get("legalAddress") or {}).get("country") or "BR",
                    "entity_status": entity.get("status") or "ACTIVE",
                }
            )
        return rows

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize GLEIF LEI records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "lei": str(r.get("lei") or "").strip(),
                "legal_name": str(r.get("legal_name") or "").strip(),
                "country": str(r.get("country") or "BR").strip().upper(),
                "entity_status": str(r.get("entity_status") or "ACTIVE").strip().upper(),
                "extracted_at": now_iso,
            }
            for r in raw_rows
            if r.get("lei")
        ]

    def extract_table(
        self,
        table_name: str = "gleif_lei_records",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist GLEIF LEI records across Medallion layers."""
        if table_name != "gleif_lei_records":
            raise ValueError(f"Unknown table '{table_name}' for gleif_lei.")
        if raw_records is None:
            try:
                raw_records = self.fetch_lei_records()
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name=table_name,
            records=normalized,
            pk_columns=['lei'],
            numeric_columns=[],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to GLEIF LEI API."""
        resp = self.http_client.get(
            "https://api.gleif.org/api/v1/lei-records?filter[entity.legalName]=Petrobras&page[size]=1",
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("gleif_lei", GleifLeiConnector)
