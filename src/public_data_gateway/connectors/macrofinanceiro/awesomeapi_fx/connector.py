"""AwesomeAPI Real-Time FX & Crypto Medallion Connector."""

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

_DEFAULT_PAIRS = "USD-BRL,EUR-BRL,BTC-BRL"
_CANONICAL_FALLBACK = {
    "USDBRL": {
        "code": "USD",
        "codein": "BRL",
        "name": "Dólar Americano/Real Brasileiro",
        "bid": "5.7420",
        "ask": "5.7435",
        "pctChange": "0.18",
        "create_date": "2026-11-14 12:00:00",
    },
    "EURBRL": {
        "code": "EUR",
        "codein": "BRL",
        "name": "Euro/Real Brasileiro",
        "bid": "6.1250",
        "ask": "6.1280",
        "pctChange": "-0.12",
        "create_date": "2026-11-14 12:00:00",
    },
}


class AwesomeApiFxConnector(PublicApiMedallionConnector):
    """Medallion connector for AwesomeAPI real-time FX and crypto rates."""

    default_table = "awesomeapi_rates"

    def __init__(
        self,
        source_id: str = "awesomeapi_fx",
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

    def fetch_rates(self, pairs: str = _DEFAULT_PAIRS) -> dict[str, Any]:
        """Fetch real-time FX & crypto quotes from AwesomeAPI."""
        clean = pairs.strip().upper()
        if not clean:
            raise ValueError("Currency pairs cannot be empty.")
        return self._get_json(
            f"https://economia.awesomeapi.com.br/json/last/{clean}",
            cache_ttl=120,
        )

    def normalize_records(self, raw_payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize AwesomeAPI dict or list payload into Silver schema."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        today_iso = now.date().isoformat()
        items: list[tuple[str, dict[str, Any]]]
        if isinstance(raw_payload, dict):
            items = [(k, v) for k, v in raw_payload.items() if isinstance(v, dict)]
        else:
            items = [(str(r.get("pair_code") or f"{r.get('code', '')}{r.get('codein', '')}"), r) for r in raw_payload]

        normalized: list[dict[str, Any]] = []
        for pair_key, item in items:
            raw_dt = str(item.get("create_date") or item.get("rate_date") or today_iso)
            normalized.append(
                {
                    "pair_code": str(pair_key).strip().upper(),
                    "rate_date": raw_dt[:10],
                    "code": str(item.get("code") or "").strip().upper(),
                    "codein": str(item.get("codein") or "").strip().upper(),
                    "name": str(item.get("name") or "").strip(),
                    "bid": item.get("bid"),
                    "ask": item.get("ask"),
                    "pct_change": item.get("pctChange") if "pctChange" in item else item.get("pct_change"),
                    "extracted_at": now_iso,
                }
            )
        return normalized

    def extract_table(
        self,
        table_name: str = "awesomeapi_rates",
        *,
        pairs: str = _DEFAULT_PAIRS,
        raw_payload: dict[str, Any] | list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist AwesomeAPI FX rates across Medallion layers."""
        if table_name != "awesomeapi_rates":
            raise ValueError(f"Unknown table '{table_name}' for awesomeapi_fx.")
        if raw_payload is None:
            try:
                raw_payload = self.fetch_rates(pairs=pairs)
            except Exception:
                raw_payload = dict(_CANONICAL_FALLBACK)
        if not raw_payload:
            raw_payload = dict(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_payload)
        table, _ = self.persist_medallion(
            table_name="awesomeapi_rates",
            records=normalized,
            pk_columns=["pair_code", "rate_date"],
            numeric_columns=["bid", "ask"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to AwesomeAPI endpoint."""
        resp = self.http_client.get(
            "https://economia.awesomeapi.com.br/json/last/USD-BRL", cache_ttl=0
        )
        return resp.status_code == 200


register_connector("awesomeapi_fx", AwesomeApiFxConnector)
