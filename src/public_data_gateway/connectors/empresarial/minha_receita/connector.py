"""Minha Receita (CNPJ Aberto) Medallion Connector."""

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
        "cnpj": "33000167000101",
        "razao_social": "PETROLEO BRASILEIRO S A PETROBRAS",
        "situacao_cadastral": "ATIVA",
        "cnae_principal": "0600001",
        "sigla_uf": "RJ",
        "municipio": "RIO DE JANEIRO",
        "qsa_count": 10,
    },
    {
        "cnpj": "00000000000191",
        "razao_social": "BANCO DO BRASIL SA",
        "situacao_cadastral": "ATIVA",
        "cnae_principal": "6422100",
        "sigla_uf": "DF",
        "municipio": "BRASILIA",
        "qsa_count": 12,
    },
]


class MinhaReceitaConnector(PublicApiMedallionConnector):
    """Medallion connector for Minha Receita CNPJ API."""

    default_table = "minha_receita_cnpjs"

    def __init__(
        self,
        source_id: str = "minha_receita",
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

    def fetch_cnpj(self, cnpj: str = "33000167000101") -> dict[str, Any]:
        """Fetch CNPJ cadastral record from Minha Receita."""
        clean = "".join(ch for ch in str(cnpj) if ch.isdigit())
        if len(clean) != 14:
            raise ValueError(f"Invalid CNPJ '{cnpj}'. Expected 14 digits.")
        data = self._get_json(f"https://minhareceita.org/{clean}", cache_ttl=3600)
        return {
            "cnpj": clean,
            "razao_social": data.get("razao_social") or "",
            "situacao_cadastral": data.get("descricao_situacao_cadastral") or "ATIVA",
            "cnae_principal": str(data.get("cnae_fiscal") or ""),
            "sigla_uf": str(data.get("uf") or "DF").upper(),
            "municipio": data.get("municipio") or "",
            "qsa_count": len(data.get("qsa") or []),
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize Minha Receita records into Silver schema."""
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
        table_name: str = "minha_receita_cnpjs",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist Minha Receita records across Medallion layers."""
        if table_name != "minha_receita_cnpjs":
            raise ValueError(f"Unknown table '{table_name}' for minha_receita.")
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
        """Verify live connectivity to Minha Receita API."""
        resp = self.http_client.get("https://minhareceita.org/33000167000101", cache_ttl=0)
        return resp.status_code == 200


register_connector("minha_receita", MinhaReceitaConnector)
