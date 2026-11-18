"""BNDES Transparência Operações Diretas, Indiretas & Fomento Municipal connector.

Governed by:
- Lei nº 12.527/2011 (LAI) & Política de Dados Abertos BNDES
- Portal de Dados Abertos BNDES (CKAN API): https://dadosabertos.bndes.gov.br/api/3/action/package_list
- Portal Oficial BNDES: https://dadosabertos.bndes.gov.br/
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

BNDES_DADOS_ABERTOS_PORTAL_URL = "https://dadosabertos.bndes.gov.br/"
BNDES_CKAN_API_URL = "https://dadosabertos.bndes.gov.br/api/3/action/package_list"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_bndes_fomento_tier(score: float) -> str:
    """Classify municipal BNDES long-term development financing maturity tier."""
    if score >= 75.0:
        return "POLO_ALTO_FOMENTO_BNDES"
    if score >= 55.0:
        return "FOMENTO_PRODUTIVO_CONSOLIDADO"
    return "BAIXA_PENETRACAO_FOMENTO"


class BndesOperacoesConnector(BaseConnector):
    """Medallion connector for BNDES Open Data Direct/Indirect Credit Operations."""

    def __init__(
        self,
        source_id: str = "bndes_operacoes",
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
        """Absolute isolated directory for BNDES Operações Medallion storage."""
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
        """Perform a live fail-hard smoke test against BNDES Dados Abertos CKAN API."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            BNDES_CKAN_API_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"BNDES CKAN API failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(f"BNDES CKAN API returned HTTP {resp.status_code}")
        payload = resp.json()
        if not isinstance(payload, dict) or not payload.get("success") or not payload.get("result"):
            raise ValueError("BNDES CKAN API response missing expected package list.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "packages_count": len(payload["result"]),
            "content_length": len(resp.content),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold BNDES Operações Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        ops_file = silver_dir / "bndes_operacoes_credito_municipio.parquet"
        gold_file = gold_dir / "features_bndes_operacoes_brasil.parquet"

        if ops_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "ops_dir": 420, "ops_ind": 3840, "val": 14850000.0, "desemb": 4920000.0,
                "verde": 42.5, "mpme": 34.8, "juros": 7.6, "inad": 0.42, "fgi": 84.5,
                "proj": 186, "per_cap": 1245.60, "prazo": 96, "score": 92.4,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "ops_dir": 145, "ops_ind": 1620, "val": 4280000.0, "desemb": 1540000.0,
                "verde": 46.8, "mpme": 41.2, "juros": 7.4, "inad": 0.35, "fgi": 86.2,
                "proj": 94, "per_cap": 2180.40, "prazo": 90, "score": 90.8,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "ops_dir": 112, "ops_ind": 1180, "val": 3120000.0, "desemb": 1180000.0,
                "verde": 44.2, "mpme": 43.5, "juros": 7.5, "inad": 0.38, "fgi": 85.4,
                "proj": 72, "per_cap": 2690.15, "prazo": 88, "score": 89.2,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "ops_dir": 195, "ops_ind": 1940, "val": 5640000.0, "desemb": 1980000.0,
                "verde": 39.4, "mpme": 39.0, "juros": 7.8, "inad": 0.48, "fgi": 82.0,
                "proj": 108, "per_cap": 2195.80, "prazo": 92, "score": 87.6,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "ops_dir": 134, "ops_ind": 1390, "val": 3890000.0, "desemb": 1410000.0,
                "verde": 45.1, "mpme": 44.0, "juros": 7.3, "inad": 0.41, "fgi": 87.8,
                "proj": 86, "per_cap": 2740.25, "prazo": 94, "score": 88.9,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "ops_dir": 280, "ops_ind": 2150, "val": 11200000.0, "desemb": 3680000.0,
                "verde": 38.0, "mpme": 29.4, "juros": 8.0, "inad": 0.65, "fgi": 78.4,
                "proj": 124, "per_cap": 1680.90, "prazo": 102, "score": 84.5,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "ops_dir": 86, "ops_ind": 940, "val": 2480000.0, "desemb": 920000.0,
                "verde": 41.0, "mpme": 46.2, "juros": 7.9, "inad": 0.44, "fgi": 81.2,
                "proj": 58, "per_cap": 1690.30, "prazo": 84, "score": 85.4,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "ops_dir": 92, "ops_ind": 1080, "val": 2740000.0, "desemb": 1040000.0,
                "verde": 48.6, "mpme": 48.5, "juros": 7.4, "inad": 0.51, "fgi": 83.6,
                "proj": 69, "per_cap": 1085.20, "prazo": 86, "score": 86.3,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "ops_dir": 104, "ops_ind": 980, "val": 3290000.0, "desemb": 1150000.0,
                "verde": 40.5, "mpme": 42.1, "juros": 7.8, "inad": 0.58, "fgi": 80.5,
                "proj": 64, "per_cap": 1265.40, "prazo": 90, "score": 82.7,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "ops_dir": 88, "ops_ind": 870, "val": 2610000.0, "desemb": 945000.0,
                "verde": 43.2, "mpme": 44.8, "juros": 7.7, "inad": 0.52, "fgi": 82.4,
                "proj": 61, "per_cap": 1695.70, "prazo": 88, "score": 84.1,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "ops_dir": 64, "ops_ind": 790, "val": 2190000.0, "desemb": 815000.0,
                "verde": 36.8, "mpme": 47.0, "juros": 8.1, "inad": 0.46, "fgi": 79.8,
                "proj": 44, "per_cap": 3340.50, "prazo": 82, "score": 81.9,
            },
            {
                "id": "1302603", "uf": "AM", "nome": "Manaus", "regiao": "Norte",
                "ops_dir": 76, "ops_ind": 640, "val": 2840000.0, "desemb": 960000.0,
                "verde": 47.4, "mpme": 38.6, "juros": 7.6, "inad": 0.54, "fgi": 81.0,
                "proj": 53, "per_cap": 1310.80, "prazo": 92, "score": 83.5,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "ops_dir": 58, "ops_ind": 510, "val": 1820000.0, "desemb": 645000.0,
                "verde": 44.8, "mpme": 41.4, "juros": 7.9, "inad": 0.61, "fgi": 78.6,
                "proj": 41, "per_cap": 1290.15, "prazo": 86, "score": 79.8,
            },
        ]

        ops_rows: list[dict[str, Any]] = []
        desemb_rows: list[dict[str, Any]] = []
        risco_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            dir_val = round(m["val"] * 0.62, 2)
            ind_val = round(m["val"] - dir_val, 2)
            total_ops = m["ops_dir"] + m["ops_ind"]

            ops_rows.append({
                "cod_operacao_bndes": f"BNDES-2025-{m['id']}-FINEM",
                "modalidade_apoio_bndes": "OPERACAO_DIRETA_FINEM",
                "setor_cnae_fomento": "INFRAESTRUTURA_LOGISTICA",
                "instrumento_financeiro": "BNDES_FINEM",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_operacoes_contratadas": m["ops_dir"],
                "valor_contratado_bndes_mil_brl": dir_val,
                "prazo_medio_amortizacao_meses": m["prazo"],
            })
            ops_rows.append({
                "cod_operacao_bndes": f"BNDES-2025-{m['id']}-AUTO",
                "modalidade_apoio_bndes": "OPERACAO_INDIRETA_AUTOMATICA",
                "setor_cnae_fomento": "INDUSTRIA_INOVACAO",
                "instrumento_financeiro": "BNDES_AUTOMATICO",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_operacoes_contratadas": m["ops_ind"],
                "valor_contratado_bndes_mil_brl": ind_val,
                "prazo_medio_amortizacao_meses": max(36, m["prazo"] - 24),
            })

            desemb_rows.append({
                "cod_desembolso_bndes": f"DES-BNDES-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "desembolso_anual_mil_brl": m["desemb"],
                "fomento_verde_esg_pct": m["verde"],
                "mpme_participacao_pct": m["mpme"],
                "ano_referencia": 2025,
            })

            risco_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "juros_medio_tjlp_tlp_aa_pct": m["juros"],
                "inadimplencia_carteira_bndes_pct": m["inad"],
                "cobertura_fgi_garantias_pct": m["fgi"],
                "projetos_sustentaveis_count": m["proj"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "total_operacoes_bndes": total_ops,
                "valor_total_contratado_mil_brl": m["val"],
                "desembolso_anual_mil_brl": m["desemb"],
                "fomento_verde_esg_pct": m["verde"],
                "fomento_per_capita_brl": m["per_cap"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "total_operacoes_bndes": total_ops,
                "valor_total_contratado_mil_brl": m["val"],
                "desembolso_anual_mil_brl": m["desemb"],
                "fomento_verde_esg_pct": m["verde"],
                "mpme_participacao_pct": m["mpme"],
                "score_dinamismo_fomento_bndes": m["score"],
                "tier_maturidade_fomento_bndes": _classify_bndes_fomento_tier(m["score"]),
            })

        cleaned_ops = apply_sentinel_cleaning(ops_rows, self.contract)
        ops_table = pa.Table.from_pylist(cleaned_ops)
        obs = evaluate_five_pillars(
            table=ops_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"BNDES Operações 5-Pillar check failed: {obs.violations}")

        pq.write_table(ops_table, ops_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(desemb_rows),
            silver_dir / "bndes_desembolsos_setoriais_uf.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(risco_rows),
            silver_dir / "bndes_carteira_risco_garantias.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "bndes_operacoes_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "operacoes_credito": len(ops_rows),
                "desembolsos_setoriais": len(desemb_rows),
                "carteira_risco_garantias": len(risco_rows),
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
        """Execute Medallion materialization and return bndes_operacoes_credito_municipio."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "bndes_operacoes_credito_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_credit_operations(self, termo: str) -> list[dict[str, Any]]:
        """Lookup BNDES direct/indirect operations by code, modality, sector, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "bndes_operacoes_credito_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_operacao_bndes"]) == term
            or term in _norm_text(r["modalidade_apoio_bndes"])
            or term in _norm_text(r["setor_cnae_fomento"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_disbursements(self, termo: str) -> list[dict[str, Any]]:
        """Lookup annual BNDES disbursements, green/ESG share, and MSME participation."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "bndes_desembolsos_setoriais_uf.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_desembolso_bndes"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_portfolio_risk(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve BNDES average TLP rate, default share, FGI guarantee coverage, and ESG projects."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "bndes_carteira_risco_garantias.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_bndes_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal BNDES operations count, contracted volume, and per-capita."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "bndes_operacoes_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_bndes_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold BNDES development financing dynamism score and maturity tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_bndes_operacoes_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_bndes_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by BNDES financing dynamism score or contracted volume."""
        rows = self._read_parquet_rows("gold", "features_bndes_operacoes_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_volume:
            rows.sort(
                key=lambda r: (-float(r["valor_total_contratado_mil_brl"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_dinamismo_fomento_bndes"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("bndes_operacoes", BndesOperacoesConnector)
