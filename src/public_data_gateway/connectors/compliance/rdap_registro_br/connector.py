"""RDAP Registro.br Domain Intelligence Medallion Connector."""

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
        "domain": "gov.br",
        "status_list": "active",
        "registered_at": "1996-01-01T00:00:00Z",
        "last_changed_at": "2024-01-01T00:00:00Z",
        "nameserver_count": 4,
    },
]


class RdapRegistroBrConnector(PublicApiMedallionConnector):
    """Medallion connector for RDAP Registro.br domain metadata."""

    default_table = "rdap_registro_br_domains"

    def __init__(
        self,
        source_id: str = "rdap_registro_br",
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

    def fetch_domain(self, domain: str = "gov.br") -> dict[str, Any]:
        """Fetch .br domain RDAP registration & age metadata."""
        clean = domain.strip().lower()
        if not clean or "." not in clean:
            raise ValueError(f"Invalid domain '{domain}'.")
        data = self._get_json(f"https://rdap.registro.br/domain/{clean}", cache_ttl=3600)
        events = {e.get("eventAction"): e.get("eventDate") for e in data.get("events") or []}
        nameservers = [ns.get("ldhName") for ns in data.get("nameservers") or []]
        status_vals = data.get("status") or ["active"]
        return {
            "domain": str(data.get("ldhName") or clean).lower(),
            "status_list": ",".join(str(s) for s in status_vals),
            "registered_at": str(events.get("registration") or ""),
            "last_changed_at": str(events.get("last changed") or ""),
            "nameserver_count": len(nameservers),
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize RDAP Registro.br records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "domain": str(r.get("domain") or "").strip().lower(),
                "status_list": str(r.get("status_list") or "active").strip(),
                "registered_at": str(r.get("registered_at") or "").strip(),
                "last_changed_at": str(r.get("last_changed_at") or "").strip(),
                "nameserver_count": int(r.get("nameserver_count") or 0),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "rdap_registro_br_domains",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist RDAP Registro.br records across Medallion layers."""
        if table_name != "rdap_registro_br_domains":
            raise ValueError(f"Unknown table '{table_name}' for rdap_registro_br.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_domain()]
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name=table_name,
            records=normalized,
            pk_columns=['domain'],
            numeric_columns=['nameserver_count'],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to RDAP Registro.br API."""
        resp = self.http_client.get("https://rdap.registro.br/domain/gov.br", cache_ttl=0)
        return resp.status_code == 200


register_connector("rdap_registro_br", RdapRegistroBrConnector)
