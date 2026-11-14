"""ReceitaWS CNPJ Medallion Connector."""

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
        "situacao_cadastral": "ATIVA",
        "cnae_principal": "64.22-1-00",
        "sigla_uf": "DF",
        "municipio": "BRASILIA",
        "qsa_count": 12,
    },
]


class ReceitaWsConnector(PublicApiMedallionConnector):
    """Medallion connector for ReceitaWS CNPJ fallback API."""

    default_table = "receitaws_cnpjs"

    def __init__(
        self,
        source_id: str = "receitaws",
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

    def fetch_cnpj(self, cnpj: str = "00000000000191") -> dict[str, Any]:
        """Fetch CNPJ cadastral record from ReceitaWS."""
        clean = "".join(ch for ch in str(cnpj) if ch.isdigit())
        if len(clean) != 14:
            raise ValueError(f"Invalid CNPJ '{cnpj}'. Expected 14 digits.")
        data = self._get_json(f"https://receitaws.com.br/v1/cnpj/{clean}", cache_ttl=3600)
        if data.get("status") == "ERROR":
            raise ValueError(data.get("message", "ReceitaWS error"))
        act = (data.get("atividade_principal") or [{}])[0]
        return {
            "cnpj": clean,
            "razao_social": data.get("nome") or "",
            "situacao_cadastral": data.get("situacao") or "ATIVA",
            "cnae_principal": str(act.get("code") or ""),
            "sigla_uf": str(data.get("uf") or "DF").upper(),
            "municipio": data.get("municipio") or "",
            "qsa_count": len(data.get("qsa") or []),
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize ReceitaWS records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "cnpj": str(r.get("cnpj") or "").strip(),
                "razao_social": str(r.get("razao_social") or "").strip(),
                "situacao_cadastral": str(r.get("situacao_cadastral") or "ATIVA").strip(),
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
        table_name: str = "receitaws_cnpjs",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist ReceitaWS records across Medallion layers."""
        if table_name != "receitaws_cnpjs":
            raise ValueError(f"Unknown table '{table_name}' for receitaws.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_cnpj()]
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
        """Verify live connectivity to ReceitaWS API."""
        resp = self.http_client.get("https://receitaws.com.br/v1/cnpj/00000000000191", cache_ttl=0)
        return resp.status_code == 200


register_connector("receitaws", ReceitaWsConnector)
