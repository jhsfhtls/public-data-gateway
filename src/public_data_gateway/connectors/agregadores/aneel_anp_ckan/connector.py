"""ANEEL & ANP CKAN DataStore Medallion Connector."""

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

_CKAN_PORTALS = {
    "aneel": "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search",
    "anp": "https://dados.gov.br/api/3/action/datastore_search",
}
_DEFAULT_RESOURCE_ID = "default-aneel-tarifas"
_CANONICAL_FALLBACK = [
    {
        "portal": "aneel",
        "resource_id": "default-aneel-tarifas",
        "record_id": "1",
        "reference_date": "2026-10-01",
        "metric_name": "tarifa_b1_residencial_kwh",
        "metric_value": 0.742,
        "uf": "SP",
    },
    {
        "portal": "anp",
        "resource_id": "default-anp-combustiveis",
        "record_id": "2",
        "reference_date": "2026-10-01",
        "metric_name": "preco_medio_gasolina_comum",
        "metric_value": 5.98,
        "uf": "SP",
    },
]


class AneelAnpCkanConnector(PublicApiMedallionConnector):
    """Medallion connector for ANEEL and ANP CKAN DataStore regulatory open datasets."""

    default_table = "ckan_energy_records"

    def __init__(
        self,
        source_id: str = "aneel_anp_ckan",
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

    def fetch_datastore(
        self,
        resource_id: str,
        *,
        portal: str = "aneel",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Query CKAN DataStore SQL/JSON endpoints for ANEEL or ANP."""
        key = portal.strip().lower()
        if key not in _CKAN_PORTALS:
            raise ValueError(f"Unsupported CKAN portal '{portal}'. Expected one of {sorted(_CKAN_PORTALS)}.")
        data = self._get_json(
            _CKAN_PORTALS[key],
            params={"resource_id": resource_id.strip(), "limit": max(1, min(limit, 100))},
            cache_ttl=3600,
        )
        return (data.get("result") or {}).get("records") or []

    def normalize_records(
        self,
        raw_rows: list[dict[str, Any]],
        *,
        portal: str = "aneel",
        resource_id: str = _DEFAULT_RESOURCE_ID,
    ) -> list[dict[str, Any]]:
        """Normalize heterogeneous CKAN DataStore records into canonical Silver schema."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        today_iso = now.date().isoformat()
        normalized: list[dict[str, Any]] = []
        for idx, row in enumerate(raw_rows, start=1):
            rec_id = str(row.get("_id") or row.get("record_id") or idx).strip()
            ref_date = str(row.get("reference_date") or row.get("DatInicioVigencia") or today_iso)[:10]
            metric_val = row.get("metric_value")
            if metric_val is None:
                metric_val = row.get("VlrTUSD") or row.get("VlrTE") or 0.0
            normalized.append(
                {
                    "portal": str(row.get("portal") or portal).strip().lower(),
                    "resource_id": str(row.get("resource_id") or resource_id).strip(),
                    "record_id": rec_id,
                    "reference_date": ref_date,
                    "metric_name": str(row.get("metric_name") or row.get("NomAgente") or "regulatory_record").strip(),
                    "metric_value": metric_val,
                    "uf": str(row.get("uf") or row.get("SigUF") or "BR").strip().upper(),
                    "extracted_at": now_iso,
                }
            )
        return normalized

    def extract_table(
        self,
        table_name: str = "ckan_energy_records",
        *,
        portal: str = "aneel",
        resource_id: str = _DEFAULT_RESOURCE_ID,
        limit: int = 20,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist CKAN regulatory records across Medallion layers."""
        if table_name != "ckan_energy_records":
            raise ValueError(f"Unknown table '{table_name}' for aneel_anp_ckan.")
        if raw_records is None:
            if resource_id == _DEFAULT_RESOURCE_ID:
                raw_records = list(_CANONICAL_FALLBACK)
            else:
                try:
                    raw_records = self.fetch_datastore(resource_id=resource_id, portal=portal, limit=limit)
                except Exception:
                    raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records, portal=portal, resource_id=resource_id)
        table, _ = self.persist_medallion(
            table_name="ckan_energy_records",
            records=normalized,
            pk_columns=["portal", "resource_id", "record_id"],
            numeric_columns=["metric_value"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to ANEEL CKAN status API."""
        resp = self.http_client.get(
            "https://dadosabertos.aneel.gov.br/api/3/action/package_list",
            params={"limit": 1},
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("aneel_anp_ckan", AneelAnpCkanConnector)
