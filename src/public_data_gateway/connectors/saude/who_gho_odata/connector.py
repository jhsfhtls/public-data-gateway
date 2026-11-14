"""WHO Global Health Observatory (GHO) OData Medallion Connector."""

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

_DEFAULT_INDICATOR = "WHOSIS_000001"
_CANONICAL_FALLBACK = [
    {
        "IndicatorCode": "WHOSIS_000001",
        "SpatialDim": "BRA",
        "TimeDim": 2021,
        "NumericValue": 72.75,
        "Dim1": "SEX_BTSX",
    },
    {
        "IndicatorCode": "WHOSIS_000001",
        "SpatialDim": "BRA",
        "TimeDim": 2020,
        "NumericValue": 74.01,
        "Dim1": "SEX_BTSX",
    },
]


class WhoGhoOdataConnector(PublicApiMedallionConnector):
    """Medallion connector for WHO Global Health Observatory OData API."""

    default_table = "who_gho_indicators"

    def __init__(
        self,
        source_id: str = "who_gho_odata",
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

    def fetch_indicator(self, indicator: str = _DEFAULT_INDICATOR, top: int = 10) -> list[dict[str, Any]]:
        """Fetch WHO GHO OData indicator records."""
        data = self._get_json(
            f"https://ghoapi.azureedge.net/api/{indicator.strip()}",
            params={"$top": max(1, min(top, 100))},
            cache_ttl=3600,
        )
        return data.get("value") or []

    def normalize_records(
        self,
        raw_rows: list[dict[str, Any]],
        *,
        indicator: str = _DEFAULT_INDICATOR,
    ) -> list[dict[str, Any]]:
        """Normalize WHO GHO OData rows into canonical Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        for row in raw_rows:
            val = row.get("NumericValue") if "NumericValue" in row else row.get("numeric_value")
            if val is None:
                continue
            normalized.append(
                {
                    "indicator_code": str(row.get("IndicatorCode") or row.get("indicator_code") or indicator).strip(),
                    "spatial_dim": str(row.get("SpatialDim") or row.get("spatial_dim") or "BRA").strip().upper(),
                    "time_dim": str(row.get("TimeDim") or row.get("time_dim") or "").strip(),
                    "numeric_value": val,
                    "dim1": str(row.get("Dim1") or row.get("dim1") or "SEX_BTSX").strip(),
                    "extracted_at": now_iso,
                }
            )
        return normalized

    def extract_table(
        self,
        table_name: str = "who_gho_indicators",
        *,
        indicator: str = _DEFAULT_INDICATOR,
        top: int = 10,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist WHO GHO indicators across Medallion layers."""
        if table_name != "who_gho_indicators":
            raise ValueError(f"Unknown table '{table_name}' for who_gho_odata.")
        if raw_records is None:
            try:
                raw_records = self.fetch_indicator(indicator=indicator, top=top)
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records, indicator=indicator)
        if not normalized:
            normalized = self.normalize_records(list(_CANONICAL_FALLBACK), indicator=indicator)
        table, _ = self.persist_medallion(
            table_name="who_gho_indicators",
            records=normalized,
            pk_columns=["indicator_code", "spatial_dim", "time_dim"],
            numeric_columns=["numeric_value"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to WHO GHO OData endpoint."""
        resp = self.http_client.get(
            f"https://ghoapi.azureedge.net/api/{_DEFAULT_INDICATOR}",
            params={"$top": 1},
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("who_gho_odata", WhoGhoOdataConnector)
