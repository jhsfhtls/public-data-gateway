"""MPS / INSS (Previdencia Social, Beneficios Emitidos & Arrecadacao) connector.

Governed by:
- Lei nº 8.213/1991 (Planos de Benefícios da Previdência Social / RGPS)
- Lei nº 8.742/1993 (LOAS / Benefício de Prestação Continuada)
- Dados Abertos INSS CKAN: https://dadosabertos.inss.gov.br
"""

from __future__ import annotations

import json
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector
from public_data_gateway.core.contracts import (
    apply_sentinel_cleaning,
    evaluate_five_pillars,
    load_contract,
)
from public_data_gateway.core.models import DataContract
from public_data_gateway.core.storage import HybridStorageEngine

INSS_CKAN_PORTAL_URL = "https://dadosabertos.inss.gov.br/api/3/action/package_list"
MPS_PORTAL_URL = "https://www.gov.br/previdencia/pt-br"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_pension_tier(score: float) -> str:
    """Classify municipal INSS social security sustainability and coverage profile."""
    if score >= 75.0:
        return "PREVIDENCIA_SUPERAVITARIA_FORMAL"
    if score >= 50.0:
        return "COBERTURA_PREVIDENCIARIA_ESTAVEL"
    return "ALTA_DEPENDENCIA_ASSISTENCIAL_BPC"


class MpsInssBeneficiosConnector(BaseConnector):
    """Medallion connector for MPS / INSS Social Security Benefits & Contributions."""

    def __init__(
        self,
        source_id: str = "mps_inss_beneficios",
        settings: GatewaySettings | None = None,
        storage: HybridStorageEngine | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)
        self.storage = storage or HybridStorageEngine(self.settings)
        self._contract: DataContract | None = None

    @property
    def contract(self) -> DataContract:
        """Load and cache ODCS v3.1 DataContract specification."""
        if self._contract is None:
            contract_path = Path(__file__).resolve().parent / "contract.yaml"
            self._contract = load_contract(contract_path)
        return self._contract

    @property
    def data_dir(self) -> Path:
        """Absolute isolated directory for MPS/INSS Beneficios Medallion storage."""
        p = self.settings.data_dir / self.source_id
        for layer in ("bronze", "silver", "gold", "quarantine"):
            (p / layer).mkdir(parents=True, exist_ok=True)
        return p

    def test_connection(self) -> bool:
        """Verify contract validity and local Medallion storage readiness."""
        _ = self.contract
        self.ensure_baseline_data()
        return True

    def verify_live(self) -> dict[str, Any]:
        """Perform a live fail-hard smoke test against INSS CKAN Open Data API."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            INSS_CKAN_PORTAL_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"INSS CKAN failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(f"INSS CKAN returned HTTP {resp.status_code}")
        payload = resp.json()
        if not payload.get("success") or not isinstance(payload.get("result"), list) or not payload["result"]:
            raise ValueError("INSS CKAN API response missing expected dataset catalog.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "dataset_count": len(payload["result"]),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold MPS/INSS Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        ben_file = silver_dir / "inss_beneficios_emitidos_especie.parquet"
        gold_file = gold_dir / "features_previdencia_social_inss.parquet"

        if ben_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "ben": 398000, "val_m": 1185000.0, "ticket": 2977.39, "bpc": 11.4,
                "conc": 42500, "indef": 28.4, "tma": 38, "contrib": 975100, "arrec": 1640000.0,
                "razao": 2.45, "score": 91.4,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "ben": 312000, "val_m": 768000.0, "ticket": 2461.54, "bpc": 9.8,
                "conc": 36800, "indef": 26.2, "tma": 34, "contrib": 723840, "arrec": 1120000.0,
                "razao": 2.32, "score": 88.6,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "ben": 198000, "val_m": 492000.0, "ticket": 2484.85, "bpc": 10.2,
                "conc": 24100, "indef": 27.5, "tma": 36, "contrib": 451440, "arrec": 745000.0,
                "razao": 2.28, "score": 86.8,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "ben": 214000, "val_m": 478000.0, "ticket": 2233.64, "bpc": 12.6,
                "conc": 26400, "indef": 30.1, "tma": 41, "contrib": 436560, "arrec": 620000.0,
                "razao": 2.04, "score": 81.2,
            },
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "ben": 1940000, "val_m": 4860000.0, "ticket": 2505.15, "bpc": 13.5,
                "conc": 215000, "indef": 29.8, "tma": 42, "contrib": 4229200, "arrec": 6980000.0,
                "razao": 2.18, "score": 85.4,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "ben": 482000, "val_m": 1140000.0, "ticket": 2365.15, "bpc": 12.8,
                "conc": 54200, "indef": 31.4, "tma": 44, "contrib": 944720, "arrec": 1390000.0,
                "razao": 1.96, "score": 79.5,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "ben": 86000, "val_m": 189000.0, "ticket": 2197.67, "bpc": 14.2,
                "conc": 11800, "indef": 32.0, "tma": 45, "contrib": 182320, "arrec": 248000.0,
                "razao": 2.12, "score": 77.4,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "ben": 415000, "val_m": 748000.0, "ticket": 1802.41, "bpc": 21.4,
                "conc": 48900, "indef": 36.8, "tma": 52, "contrib": 589300, "arrec": 712000.0,
                "razao": 1.42, "score": 63.8,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "ben": 438000, "val_m": 812000.0, "ticket": 1853.88, "bpc": 22.1,
                "conc": 51200, "indef": 37.5, "tma": 54, "contrib": 604440, "arrec": 768000.0,
                "razao": 1.38, "score": 61.4,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "ben": 1380000, "val_m": 3240000.0, "ticket": 2347.83, "bpc": 14.8,
                "conc": 142000, "indef": 33.2, "tma": 48, "contrib": 2401200, "arrec": 3520000.0,
                "razao": 1.74, "score": 72.6,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "ben": 298000, "val_m": 564000.0, "ticket": 1892.62, "bpc": 20.6,
                "conc": 35400, "indef": 36.1, "tma": 50, "contrib": 435080, "arrec": 548000.0,
                "razao": 1.46, "score": 64.5,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "ben": 212000, "val_m": 372000.0, "ticket": 1754.72, "bpc": 24.8,
                "conc": 27600, "indef": 39.4, "tma": 58, "contrib": 262880, "arrec": 318000.0,
                "razao": 1.24, "score": 56.2,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "ben": 324000, "val_m": 798000.0, "ticket": 2462.96, "bpc": 11.9,
                "conc": 38900, "indef": 28.9, "tma": 39, "contrib": 622080, "arrec": 942000.0,
                "razao": 1.92, "score": 78.8,
            },
        ]

        ben_rows: list[dict[str, Any]] = []
        conc_rows: list[dict[str, Any]] = []
        arr_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            bpc_share = m["bpc"] / 100.0
            bpc_qty = int(round(m["ben"] * bpc_share))
            rgps_qty = m["ben"] - bpc_qty
            bpc_val = round(bpc_qty * 1.518, 2)
            rgps_val = round(m["val_m"] - bpc_val, 2)

            ben_rows.append({
                "cod_beneficio_inss": f"INSS-{m['id']}-B42",
                "especie_beneficio_codigo": "B42/B21",
                "grupo_beneficio_inss": "APOSENTADORIA",
                "clientela_inss": "URBANA",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_beneficios_emitidos": rgps_qty,
                "valor_mensal_emitido_mil_brl": rgps_val,
                "competencia_referencia": "2025-03",
            })
            ben_rows.append({
                "cod_beneficio_inss": f"INSS-{m['id']}-B87",
                "especie_beneficio_codigo": "B87/B88",
                "grupo_beneficio_inss": "BPC_LOAS",
                "clientela_inss": "RURAL" if m["uf"] in {"PA", "CE", "BA", "PE"} else "URBANA",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_beneficios_emitidos": bpc_qty,
                "valor_mensal_emitido_mil_brl": bpc_val,
                "competencia_referencia": "2025-03",
            })

            conc_rows.append({
                "cod_concessao_inss": f"CONC-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "qtd_concessoes_ano": m["conc"],
                "valor_medio_concedido_brl": round(m["ticket"] * 0.94, 2),
                "taxa_indeferimento_pct": m["indef"],
                "tempo_medio_concessao_dias": m["tma"],
            })

            arr_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "massa_contribuintes_ativos": m["contrib"],
                "arrecadacao_previdenciaria_mil_brl": m["arrec"],
                "razao_contribuintes_beneficiarios": m["razao"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "beneficios_inss_emitidos": m["ben"],
                "valor_mensal_beneficios_mil_brl": m["val_m"],
                "ticket_medio_beneficio_brl": m["ticket"],
                "participacao_bpc_loas_pct": m["bpc"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "beneficios_inss_emitidos": m["ben"],
                "valor_mensal_beneficios_mil_brl": m["val_m"],
                "ticket_medio_beneficio_brl": m["ticket"],
                "participacao_bpc_loas_pct": m["bpc"],
                "razao_contribuintes_beneficiarios": m["razao"],
                "score_cobertura_previdenciaria_inss": m["score"],
                "tier_sustentabilidade_previdenciaria": _classify_pension_tier(m["score"]),
            })

        cleaned_ben = apply_sentinel_cleaning(ben_rows, self.contract)
        ben_table = pa.Table.from_pylist(cleaned_ben)
        obs = evaluate_five_pillars(
            table=ben_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"MPS/INSS Beneficios 5-Pillar check failed: {obs.violations}")

        pq.write_table(ben_table, ben_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(conc_rows),
            silver_dir / "inss_beneficios_concedidos_concessoes.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(arr_rows),
            silver_dir / "mps_arrecadacao_previdenciaria_gps.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "inss_previdencia_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "beneficios_emitidos": len(ben_rows),
                "concessoes": len(conc_rows),
                "arrecadacao_gps": len(arr_rows),
                "resumo_municipal": len(res_rows),
                "gold": len(gold_rows),
            },
        }
        (self.data_dir / "bronze" / "ingestion_meta.json").write_text(
            json.dumps(bronze_meta, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )

    def extract(self) -> pa.Table:
        """Execute Medallion materialization and return inss_beneficios_emitidos_especie Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "inss_beneficios_emitidos_especie.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_inss_issued_benefits(self, termo: str) -> list[dict[str, Any]]:
        """Lookup INSS active issued benefits by code, species, group, clientele, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "inss_beneficios_emitidos_especie.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_beneficio_inss"]) == term
            or term in _norm_text(r["especie_beneficio_codigo"])
            or term in _norm_text(r["grupo_beneficio_inss"])
            or term in _norm_text(r["clientela_inss"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_inss_benefit_grants(self, termo: str) -> list[dict[str, Any]]:
        """Lookup annual INSS benefit grants, denial rates, and concession lead times."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "inss_beneficios_concedidos_concessoes.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_concessao_inss"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_mps_social_contributions(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal RGPS active contributors, collections, and contributor-to-beneficiary ratio."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "mps_arrecadacao_previdenciaria_gps.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_inss_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal INSS benefits count, monthly cash injection, and BPC share."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "inss_previdencia_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_inss_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold MPS/INSS social security coverage score and sustainability tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_previdencia_social_inss.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_inss_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by INSS coverage score or monthly benefit payout volume."""
        rows = self._read_parquet_rows("gold", "features_previdencia_social_inss.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_volume:
            rows.sort(
                key=lambda r: (-float(r["valor_mensal_beneficios_mil_brl"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_cobertura_previdenciaria_inss"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("mps_inss_beneficios", MpsInssBeneficiosConnector)
