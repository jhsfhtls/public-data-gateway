"""Upgini Public Feature Enrichment Medallion Connector."""

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
    {"feature_group_id": "upgini_macro_calendar_br", "provider": "upgini", "domain": "macrofinanceiro", "coverage_scope": "BR_NATIONAL", "status": "CATALOGED"},
    {"feature_group_id": "upgini_weather_holidays_global", "provider": "upgini", "domain": "ambiental", "coverage_scope": "GLOBAL", "status": "CATALOGED"},
]


class UpginiAdapterConnector(PublicApiMedallionConnector):
    """Medallion connector for Upgini automated public ML feature groups."""

    default_table = "upgini_feature_catalog"

    def __init__(
        self,
        source_id: str = "upgini_adapter",
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

    def describe_features(self) -> dict[str, Any]:
        """Return Upgini adapter metadata and available public feature groups."""
        return {
            "provider": "upgini",
            "status": "CATALOGED",
            "feature_groups": list(_CANONICAL_FALLBACK),
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize Upgini feature group catalog rows into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "feature_group_id": str(r.get("feature_group_id") or "").strip(),
                "provider": str(r.get("provider") or "upgini").strip(),
                "domain": str(r.get("domain") or "agregadores").strip(),
                "coverage_scope": str(r.get("coverage_scope") or "GLOBAL").strip(),
                "status": str(r.get("status") or "CATALOGED").strip(),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "upgini_feature_catalog",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Persist Upgini feature group catalog across Medallion layers."""
        if table_name != "upgini_feature_catalog":
            raise ValueError(f"Unknown table '{table_name}' for upgini_adapter.")
        records = raw_records if raw_records is not None else list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(records)
        table, _ = self.persist_medallion(
            table_name="upgini_feature_catalog",
            records=normalized,
            pk_columns=["feature_group_id"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live reachability of Upgini PyPI package metadata endpoint."""
        resp = self.http_client.get("https://pypi.org/pypi/upgini/json", cache_ttl=0)
        return resp.status_code == 200


register_connector("upgini_adapter", UpginiAdapterConnector)
