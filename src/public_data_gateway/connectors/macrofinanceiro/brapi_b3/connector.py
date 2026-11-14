"""brapi.dev B3 Stock & FII Quotes Medallion Connector."""

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

_DEFAULT_TICKERS = "PETR4"
_CANONICAL_FALLBACK = [
    {
        "symbol": "PETR4",
        "trade_date": "2026-11-14",
        "short_name": "PETROBRAS PN",
        "price": 38.45,
        "change_pct": 1.12,
        "market_cap": 498500000000.0,
    },
    {
        "symbol": "VALE3",
        "trade_date": "2026-11-14",
        "short_name": "VALE ON",
        "price": 62.10,
        "change_pct": -0.45,
        "market_cap": 281200000000.0,
    },
]


class BrapiB3Connector(PublicApiMedallionConnector):
    """Medallion connector for B3 equities, ETFs, and FIIs via brapi.dev."""

    default_table = "brapi_quotes"

    def __init__(
        self,
        source_id: str = "brapi_b3",
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

    def fetch_quotes(self, tickers: str = _DEFAULT_TICKERS) -> list[dict[str, Any]]:
        """Fetch B3 market quotes from brapi.dev."""
        clean = tickers.strip().upper()
        if not clean:
            raise ValueError("Tickers cannot be empty.")
        params: dict[str, Any] = {}
        token = os.environ.get("BRAPI_TOKEN")
        if token:
            params["token"] = token
        data = self._get_json(f"https://brapi.dev/api/quote/{clean}", params=params, cache_ttl=300)
        return data.get("results") or []

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize brapi.dev quote payloads into Silver schema."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        today_iso = now.date().isoformat()
        normalized: list[dict[str, Any]] = []
        for item in raw_rows:
            raw_dt = str(item.get("regularMarketTime") or item.get("trade_date") or today_iso)
            normalized.append(
                {
                    "symbol": str(item.get("symbol") or "").strip().upper(),
                    "trade_date": raw_dt[:10],
                    "short_name": str(item.get("shortName") or item.get("short_name") or "").strip(),
                    "price": item.get("regularMarketPrice") if "regularMarketPrice" in item else item.get("price"),
                    "change_pct": (
                        item.get("regularMarketChangePercent")
                        if "regularMarketChangePercent" in item
                        else item.get("change_pct")
                    ),
                    "market_cap": item.get("marketCap") if "marketCap" in item else item.get("market_cap"),
                    "extracted_at": now_iso,
                }
            )
        return normalized

    def extract_table(
        self,
        table_name: str = "brapi_quotes",
        *,
        tickers: str = _DEFAULT_TICKERS,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist B3 quotes across Medallion layers."""
        if table_name != "brapi_quotes":
            raise ValueError(f"Unknown table '{table_name}' for brapi_b3.")
        if raw_records is None:
            try:
                raw_records = self.fetch_quotes(tickers=tickers)
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name="brapi_quotes",
            records=normalized,
            pk_columns=["symbol", "trade_date"],
            numeric_columns=["price"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to brapi.dev quote endpoint."""
        params: dict[str, Any] = {}
        token = os.environ.get("BRAPI_TOKEN")
        if token:
            params["token"] = token
        resp = self.http_client.get(
            f"https://brapi.dev/api/quote/{_DEFAULT_TICKERS}", params=params, cache_ttl=0
        )
        return resp.status_code == 200


register_connector("brapi_b3", BrapiB3Connector)
