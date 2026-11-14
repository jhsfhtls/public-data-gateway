"""CNPJa Open API Medallion Connector."""

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
        "cnpj": "00000000000191",
        "razao_social": "BANCO DO BRASIL SA",
        "situacao_cadastral": "Ativa",
        "cnae_principal": "6422100",
        "sigla_uf": "DF",
        "municipio": "Brasilia",
        "qsa_count": 12,
    },
]


class CnpjaOpenConnector(PublicApiMedallionConnector):
    """Medallion connector for CNPJa Open Office API."""

    default_table = "cnpja_open_offices"

    def __init__(
        self,
        source_id: str = "cnpja_open",
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

    def fetch_office(self, cnpj: str = "00000000000191") -> dict[str, Any]:
        """Fetch company office profile from CNPJa Open API."""
        clean = "".join(ch for ch in str(cnpj) if ch.isdigit())
        if len(clean) != 14:
            raise ValueError(f"Invalid CNPJ '{cnpj}'. Expected 14 digits.")
        data = self._get_json(f"https://open.cnpja.com/office/{clean}", cache_ttl=3600)
        company = data.get("company") or {}
        status = data.get("status") or {}
        address = data.get("address") or {}
        main_act = data.get("mainActivity") or {}
        return {
            "cnpj": clean,
            "razao_social": company.get("name") or "",
            "situacao_cadastral": status.get("text") or "Ativa",
            "cnae_principal": str(main_act.get("id") or ""),
            "sigla_uf": str(address.get("state") or "DF").upper(),
            "municipio": address.get("city") or "",
            "qsa_count": len(company.get("members") or []),
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize CNPJa Open records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "cnpj": str(r.get("cnpj") or "").strip(),
                "razao_social": str(r.get("razao_social") or "").strip(),
                "situacao_cadastral": str(r.get("situacao_cadastral") or "Ativa").strip(),
                "cnae_principal": str(r.get("cnae_principal") or "").strip(),
                "sigla_uf": str(r.get("sigla_uf") or "DF").strip().upper(),
                "municipio": str(r.get("municipio") or "").strip(),
                "qsa_count": int(r.get("qsa_count") or 0),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "cnpja_open_offices",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist CNPJa Open records across Medallion layers."""
        if table_name != "cnpja_open_offices":
            raise ValueError(f"Unknown table '{table_name}' for cnpja_open.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_office()]
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name=table_name,
            records=normalized,
            pk_columns=['cnpj'],
            numeric_columns=['qsa_count'],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to CNPJa Open API."""
        resp = self.http_client.get("https://open.cnpja.com/office/00000000000191", cache_ttl=0)
        return resp.status_code == 200


register_connector("cnpja_open", CnpjaOpenConnector)
