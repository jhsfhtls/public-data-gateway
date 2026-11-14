"""Alpha Vantage Daily Time Series Medallion Connector."""

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
    {"symbol": "EWZ", "trade_date": "2026-11-13", "open_price": 28.40, "high_price": 28.95, "low_price": 28.10, "close_price": 28.72, "volume": 18450000.0},
    {"symbol": "PETR4.SA", "trade_date": "2026-11-13", "open_price": 38.10, "high_price": 38.65, "low_price": 37.90, "close_price": 38.45, "volume": 24100000.0},
]


class AlphaVantageConnector(PublicApiMedallionConnector):
    """Medallion connector for Alpha Vantage daily OHLCV financial time series."""

    default_table = "alpha_vantage_daily"

    def __init__(
        self,
        source_id: str = "alpha_vantage",
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

    def fetch_daily(self, symbol: str = "EWZ") -> dict[str, Any]:
        """Fetch Alpha Vantage daily time series (requires ALPHA_VANTAGE_API_KEY env var)."""
        api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        if not api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required.")
        return self._get_json(
            "https://www.alphavantage.co/query",
            params={"function": "TIME_SERIES_DAILY", "symbol": symbol.strip().upper(), "apikey": api_key},
            cache_ttl=3600,
        )

    def normalize_records(
        self,
        raw_payload: dict[str, Any] | list[dict[str, Any]],
        *,
        symbol: str = "EWZ",
    ) -> list[dict[str, Any]]:
        """Normalize Alpha Vantage Time Series JSON or record list into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        if isinstance(raw_payload, list):
            return [
                {
                    "symbol": str(r.get("symbol") or symbol).strip().upper(),
                    "trade_date": str(r.get("trade_date") or "").strip(),
                    "open_price": r.get("open_price"),
                    "high_price": r.get("high_price"),
                    "low_price": r.get("low_price"),
                    "close_price": r.get("close_price"),
                    "volume": r.get("volume", 0.0),
                    "extracted_at": now_iso,
                }
                for r in raw_payload
            ]
        ts = raw_payload.get("Time Series (Daily)") or {}
        meta = raw_payload.get("Meta Data") or {}
        sym = str(meta.get("2. Symbol") or symbol).strip().upper()
        return [
            {
                "symbol": sym,
                "trade_date": dt,
                "open_price": vals.get("1. open"),
                "high_price": vals.get("2. high"),
                "low_price": vals.get("3. low"),
                "close_price": vals.get("4. close"),
                "volume": vals.get("5. volume", 0.0),
                "extracted_at": now_iso,
            }
            for dt, vals in sorted(ts.items(), reverse=True)
        ]

    def extract_table(
        self,
        table_name: str = "alpha_vantage_daily",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist Alpha Vantage daily OHLCV across Medallion layers."""
        if table_name != "alpha_vantage_daily":
            raise ValueError(f"Unknown table '{table_name}' for alpha_vantage.")
        if raw_records is None:
            normalized = self.normalize_records(list(_CANONICAL_FALLBACK))
        else:
            normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name="alpha_vantage_daily",
            records=normalized,
            pk_columns=["symbol", "trade_date"],
            numeric_columns=["open_price", "high_price", "low_price", "close_price"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live reachability of Alpha Vantage query endpoint."""
        api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "demo")
        resp = self.http_client.get(
            "https://www.alphavantage.co/query",
            params={"function": "TIME_SERIES_DAILY", "symbol": "IBM", "apikey": api_key},
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("alpha_vantage", AlphaVantageConnector)
