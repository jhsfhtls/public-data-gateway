"""CompanyEnrich / Apollo / BigDataCorp Freemium B2B Adapter Medallion Connector."""

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
    {
        "domain": "petrobras.com.br",
        "company_name": "Petrobras",
        "industry": "Oil & Energy",
        "country_code": "BR",
        "employee_range": "10001+",
        "provider": "freemium_catalog_fallback",
    },
]


class CompanyEnrichConnector(PublicApiMedallionConnector):
    """Medallion connector for optional freemium B2B domain enrichment."""

    default_table = "company_enrich_profiles"

    def __init__(
        self,
        source_id: str = "company_enrich_apollo_bdc",
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

    def fetch_b2b_profile(self, domain: str = "petrobras.com.br") -> dict[str, Any]:
        """Fetch B2B company profile when API key is configured, or return structured fallback."""
        clean = domain.strip().lower()
        if not clean or "." not in clean:
            raise ValueError(f"Invalid domain '{domain}'.")
        api_key = os.getenv("COMPANY_ENRICH_API_KEY") or os.getenv("APOLLO_API_KEY")
        if not api_key:
            return {
                "domain": clean,
                "company_name": clean.split(".")[0].title(),
                "industry": "B2B Enterprise",
                "country_code": "BR" if clean.endswith(".br") else "US",
                "employee_range": "unknown",
                "provider": "unauthenticated_placeholder",
            }
        data = self._get_json(
            "https://api.companyenrich.com/companies/enrich",
            params={"domain": clean},
            headers={"Authorization": f"Bearer {api_key}"},
            cache_ttl=3600,
        )
        return {
            "domain": clean,
            "company_name": data.get("name") or clean,
            "industry": data.get("industry") or "unknown",
            "country_code": str((data.get("location") or {}).get("country_code") or "BR").upper(),
            "employee_range": str(data.get("employees") or "unknown"),
            "provider": "company_enrich",
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize B2B profile records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "domain": str(r.get("domain") or "").strip().lower(),
                "company_name": str(r.get("company_name") or "").strip(),
                "industry": str(r.get("industry") or "unknown").strip(),
                "country_code": str(r.get("country_code") or "BR").strip().upper(),
                "employee_range": str(r.get("employee_range") or "unknown").strip(),
                "provider": str(r.get("provider") or "freemium_catalog_fallback").strip(),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "company_enrich_profiles",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist B2B domain profiles across Medallion layers."""
        if table_name != "company_enrich_profiles":
            raise ValueError(f"Unknown table '{table_name}' for company_enrich_apollo_bdc.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_b2b_profile()]
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name=table_name,
            records=normalized,
            pk_columns=['domain'],
            numeric_columns=[],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live reachability of CompanyEnrich endpoint (200 or 401 when unauthenticated)."""
        resp = self.http_client.get(
            "https://api.companyenrich.com/companies/enrich?domain=openai.com",
            cache_ttl=0,
        )
        return resp.status_code in (200, 401)


register_connector("company_enrich_apollo_bdc", CompanyEnrichConnector)
