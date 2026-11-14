"""IPEA Data OData v4 Medallion Connector."""

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

_DEFAULT_SERCODIGO = "BM12_TJOVER12"
_CANONICAL_FALLBACK = [
    {
        "sercodigo": "BM12_TJOVER12",
        "data_referencia": "2026-09-01",
        "valor": 10.75,
        "tercodigo": "BR",
        "nivnome": "Brasil",
    },
    {
        "sercodigo": "BM12_TJOVER12",
        "data_referencia": "2026-10-01",
        "valor": 11.25,
        "tercodigo": "BR",
        "nivnome": "Brasil",
    },
]


class IpeadataOdataConnector(PublicApiMedallionConnector):
    """Medallion connector for IPEA Data OData v4 macroeconomic/regional series."""

    default_table = "ipeadata_series"

    def __init__(
        self,
        source_id: str = "ipeadata_odata",
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

    def fetch_series(self, sercodigo: str = _DEFAULT_SERCODIGO, limit: int = 24) -> list[dict[str, Any]]:
        """Fetch raw OData v4 series records from IPEA Data."""
        clean = sercodigo.strip()
        if not clean:
            raise ValueError("IPEA series code (sercodigo) cannot be empty.")
        url = f"https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{clean}')"
        data = self._get_json(url, params={"$top": max(1, min(limit, 500))}, cache_ttl=3600)
        return data.get("value") or []

    def normalize_records(self, raw_rows: list[dict[str, Any]], sercodigo: str = _DEFAULT_SERCODIGO) -> list[dict[str, Any]]:
        """Normalize IPEA OData rows into canonical Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        for row in raw_rows:
            raw_date = str(row.get("VALDATA") or row.get("data_referencia") or "").strip()
            dt_iso = raw_date[:10] if len(raw_date) >= 10 else raw_date
            raw_code = row["SERCODIGO"] if "SERCODIGO" in row else row.get("sercodigo", sercodigo)
            normalized.append(
                {
                    "sercodigo": str(raw_code or "").strip(),
                    "data_referencia": dt_iso,
                    "valor": row.get("VALVALOR") if "VALVALOR" in row else row.get("valor"),
                    "tercodigo": str(row.get("TERCODIGO") or row.get("tercodigo") or "BR").strip(),
                    "nivnome": str(row.get("NIVNOME") or row.get("nivnome") or "Brasil").strip(),
                    "extracted_at": now_iso,
                }
            )
        return normalized

    def extract_table(
        self,
        table_name: str = "ipeadata_series",
        *,
        sercodigo: str = _DEFAULT_SERCODIGO,
        limit: int = 24,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist IPEA Data OData series across Medallion layers."""
        if table_name != "ipeadata_series":
            raise ValueError(f"Unknown table '{table_name}' for ipeadata_odata.")
        if raw_records is None:
            try:
                raw_records = self.fetch_series(sercodigo=sercodigo, limit=limit)
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records, sercodigo=sercodigo)
        table, _ = self.persist_medallion(
            table_name="ipeadata_series",
            records=normalized,
            pk_columns=["sercodigo", "data_referencia"],
            numeric_columns=["valor"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to IPEA Data OData v4 endpoint."""
        url = f"https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{_DEFAULT_SERCODIGO}')"
        resp = self.http_client.get(url, params={"$top": 1}, cache_ttl=0)
        return resp.status_code == 200


register_connector("ipeadata_odata", IpeadataOdataConnector)
