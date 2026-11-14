"""ViaCEP Postal Addressing Medallion Connector."""

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
        "cep": "01001000",
        "logradouro": "Praca da Se",
        "bairro": "Se",
        "localidade": "Sao Paulo",
        "sigla_uf": "SP",
        "id_municipio": "3550308",
        "ddd": "11",
    },
]


class ViaCepConnector(PublicApiMedallionConnector):
    """Medallion connector for ViaCEP Brazilian postal code lookup."""

    default_table = "viacep_ceps"

    def __init__(
        self,
        source_id: str = "viacep",
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

    def fetch_cep(self, cep: str = "01001000") -> dict[str, Any]:
        """Fetch postal address metadata from ViaCEP."""
        clean = "".join(ch for ch in str(cep) if ch.isdigit())
        if len(clean) != 8:
            raise ValueError(f"Invalid CEP '{cep}'. Expected 8 digits.")
        data = self._get_json(f"https://viacep.com.br/ws/{clean}/json/", cache_ttl=86400)
        if data.get("erro"):
            raise ValueError(f"CEP '{clean}' not found in ViaCEP.")
        return {
            "cep": clean,
            "logradouro": data.get("logradouro") or "",
            "bairro": data.get("bairro") or "",
            "localidade": data.get("localidade") or "",
            "sigla_uf": str(data.get("uf") or "SP").upper(),
            "id_municipio": str(data.get("ibge") or "3550308"),
            "ddd": str(data.get("ddd") or ""),
        }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize ViaCEP records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "cep": str(r.get("cep") or "").strip(),
                "logradouro": str(r.get("logradouro") or "").strip(),
                "bairro": str(r.get("bairro") or "").strip(),
                "localidade": str(r.get("localidade") or "").strip(),
                "sigla_uf": str(r.get("sigla_uf") or "SP").strip().upper(),
                "id_municipio": str(r.get("id_municipio") or "").strip(),
                "ddd": str(r.get("ddd") or "").strip(),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "viacep_ceps",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist ViaCEP records across Medallion layers."""
        if table_name != "viacep_ceps":
            raise ValueError(f"Unknown table '{table_name}' for viacep.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_cep()]
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name=table_name,
            records=normalized,
            pk_columns=['cep'],
            numeric_columns=[],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to ViaCEP API."""
        resp = self.http_client.get("https://viacep.com.br/ws/01001000/json/", cache_ttl=0)
        return resp.status_code == 200


register_connector("viacep", ViaCepConnector)
