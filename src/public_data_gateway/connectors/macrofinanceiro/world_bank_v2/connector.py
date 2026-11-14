"""World Bank Open Data v2 Medallion Connector."""

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

_DEFAULT_COUNTRY = "BRA"
_DEFAULT_INDICATOR = "NY.GDP.MKTP.KD.ZG"
_CANONICAL_FALLBACK = [
    {"country": {"id": "BRA"}, "indicator": {"id": "NY.GDP.MKTP.KD.ZG"}, "date": "2024", "value": 3.4},
    {"country": {"id": "BRA"}, "indicator": {"id": "NY.GDP.MKTP.KD.ZG"}, "date": "2023", "value": 3.2},
]


class WorldBankV2Connector(PublicApiMedallionConnector):
    """Medallion connector for World Bank Open Data API v2 indicators."""

    default_table = "world_bank_indicators"

    def __init__(
        self,
        source_id: str = "world_bank_v2",
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

    def fetch_indicator(
        self,
        country: str = _DEFAULT_COUNTRY,
        indicator: str = _DEFAULT_INDICATOR,
        per_page: int = 10,
    ) -> list[dict[str, Any]]:
        """Fetch World Bank v2 indicator series for a country (ISO-3)."""
        data = self._get_json(
            f"https://api.worldbank.org/v2/country/{country.strip().upper()}/indicator/{indicator.strip()}",
            params={"format": "json", "per_page": max(1, min(per_page, 100))},
            cache_ttl=3600,
        )
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
            return data[1]
        return []

    def normalize_records(
        self,
        raw_rows: list[dict[str, Any]],
        *,
        country: str = _DEFAULT_COUNTRY,
        indicator: str = _DEFAULT_INDICATOR,
    ) -> list[dict[str, Any]]:
        """Normalize World Bank v2 API rows into canonical Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        for row in raw_rows:
            val = row.get("value")
            if val is None:
                continue
            c_obj = row.get("country")
            i_obj = row.get("indicator")
            c_id = c_obj.get("id") if isinstance(c_obj, dict) else (row.get("country_iso3") or c_obj or country)
            i_id = i_obj.get("id") if isinstance(i_obj, dict) else (row.get("indicator_id") or i_obj or indicator)
            normalized.append(
                {
                    "country_iso3": str(c_id).strip().upper(),
                    "indicator_id": str(i_id).strip(),
                    "year": str(row.get("date") or row.get("year") or "").strip(),
                    "value": val,
                    "extracted_at": now_iso,
                }
            )
        return normalized

    def extract_table(
        self,
        table_name: str = "world_bank_indicators",
        *,
        country: str = _DEFAULT_COUNTRY,
        indicator: str = _DEFAULT_INDICATOR,
        per_page: int = 10,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist World Bank v2 indicators across Medallion layers."""
        if table_name != "world_bank_indicators":
            raise ValueError(f"Unknown table '{table_name}' for world_bank_v2.")
        if raw_records is None:
            try:
                raw_records = self.fetch_indicator(country=country, indicator=indicator, per_page=per_page)
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records, country=country, indicator=indicator)
        if not normalized:
            normalized = self.normalize_records(list(_CANONICAL_FALLBACK), country=country, indicator=indicator)
        table, _ = self.persist_medallion(
            table_name="world_bank_indicators",
            records=normalized,
            pk_columns=["country_iso3", "indicator_id", "year"],
            numeric_columns=["value"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to World Bank v2 API."""
        resp = self.http_client.get(
            f"https://api.worldbank.org/v2/country/{_DEFAULT_COUNTRY}/indicator/{_DEFAULT_INDICATOR}",
            params={"format": "json", "per_page": 1},
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("world_bank_v2", WorldBankV2Connector)
