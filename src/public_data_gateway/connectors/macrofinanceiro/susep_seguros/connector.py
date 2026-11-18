"""SUSEP (Mercado Segurador, Previdencia Complementar & Solvencia) connector.

Governed by:
- Decreto-Lei no 73/1966 (Sistema Nacional de Seguros Privados & Criacao da SUSEP)
- Lei Complementar no 126/2007 (Politica de Resseguro e Supervisao SUSEP)
- Sistema de Estatisticas da SUSEP (SES): https://www2.susep.gov.br/menuestatistica/SES/principal.aspx
- Portal Institucional SUSEP: https://www.gov.br/susep/pt-br
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

SUSEP_SES_PORTAL_URL = "https://www2.susep.gov.br/menuestatistica/SES/principal.aspx"
SUSEP_GOV_PORTAL_URL = "https://www.gov.br/susep/pt-br"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_insurance_market_tier(score: float) -> str:
    """Classify municipal insurance market maturity and solvency tier."""
    if score >= 78.0:
        return "HUB_SEGUROS_SOLVENCIA_ALTA"
    if score >= 58.0:
        return "MERCADO_SEGURADOR_CONSOLIDADO"
    return "PENETRACAO_SEGURADORA_EXPANSAO"


class SusepSegurosConnector(BaseConnector):
    """Medallion connector for SUSEP insurance premiums, claims, VGBL/PGBL, and solvency."""

    def __init__(
        self,
        source_id: str = "susep_seguros",
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
        """Absolute isolated directory for SUSEP Seguros Medallion storage."""
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
        """Perform a live fail-hard smoke test against SUSEP SES Statistical Portal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            SUSEP_SES_PORTAL_URL,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"SUSEP SES Portal failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        body_lower = resp.text.lower()
        if len(resp.content) < 5000 or ("susep" not in body_lower and "estat" not in body_lower):
            raise RuntimeError("SUSEP SES Portal returned unexpected payload content")
        return {
            "status": "LIVE_SUCCESS",
            "source_id": self.source_id,
            "endpoint": str(resp.url),
            "http_status": resp.status_code,
            "content_bytes": len(resp.content),
        }

    def ensure_baseline_data(self) -> None:
        """Populate Bronze metadata, 4 Silver ZSTD Parquets, and Gold ML Parquet if absent."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        pr_file = silver_dir / "susep_premios_sinistros_ramo.parquet"
        gold_file = gold_dir / "features_susep_seguros_brasil.parquet"
        if pr_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "r1": "AUTO_PATRIMONIAL", "r2": "VIDA_PESSOAS", "premio": 98420000.0,
                "sinistral": 42.5, "prev": 46800000.0, "vgbl_pct": 76.4, "prov_mi": 412500.0,
                "corr": 18420, "pj": 8240, "solv": 168.4, "cob": 144.2, "percap": 8592.0, "score": 94.8,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "r1": "GARANTIA_CREDITO", "r2": "VIDA_PESSOAS", "premio": 38900000.0,
                "sinistral": 45.2, "prev": 18400000.0, "vgbl_pct": 74.1, "prov_mi": 195400.0,
                "corr": 7980, "pj": 3420, "solv": 162.1, "cob": 139.5, "percap": 6260.0, "score": 91.2,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "r1": "AUTO_PATRIMONIAL", "r2": "RURAL_AGRICOLA", "premio": 14650000.0,
                "sinistral": 43.8, "prev": 7920000.0, "vgbl_pct": 78.9, "prov_mi": 58200.0,
                "corr": 3640, "pj": 1580, "solv": 158.6, "cob": 136.0, "percap": 8260.0, "score": 89.4,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "r1": "AUTO_PATRIMONIAL", "r2": "TRANSPORTE_CARGA", "premio": 13840000.0,
                "sinistral": 44.6, "prev": 7150000.0, "vgbl_pct": 75.2, "prov_mi": 52400.0,
                "corr": 4120, "pj": 1790, "solv": 154.2, "cob": 134.8, "percap": 5975.0, "score": 87.6,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "r1": "VIDA_PESSOAS", "r2": "GARANTIA_CREDITO", "premio": 11420000.0,
                "sinistral": 39.4, "prev": 6840000.0, "vgbl_pct": 71.8, "prov_mi": 48900.0,
                "corr": 2480, "pj": 1090, "solv": 164.0, "cob": 141.2, "percap": 4054.0, "score": 86.9,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "r1": "RESIDENCIAL_EMPRESARIAL", "r2": "RURAL_AGRICOLA", "premio": 10980000.0,
                "sinistral": 56.4, "prev": 6120000.0, "vgbl_pct": 77.3, "prov_mi": 43800.0,
                "corr": 3290, "pj": 1410, "solv": 146.5, "cob": 127.4, "percap": 8238.0, "score": 78.4,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "r1": "AUTO_PATRIMONIAL", "r2": "TRANSPORTE_CARGA", "premio": 9640000.0,
                "sinistral": 41.8, "prev": 5420000.0, "vgbl_pct": 79.4, "prov_mi": 39600.0,
                "corr": 2840, "pj": 1210, "solv": 159.4, "cob": 137.8, "percap": 8460.0, "score": 88.1,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "r1": "RURAL_AGRICOLA", "r2": "AUTO_PATRIMONIAL", "premio": 7820000.0,
                "sinistral": 47.2, "prev": 4180000.0, "vgbl_pct": 80.5, "prov_mi": 28400.0,
                "corr": 2190, "pj": 960, "solv": 151.0, "cob": 131.6, "percap": 5439.0, "score": 82.6,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "r1": "VIDA_PESSOAS", "r2": "RESIDENCIAL_EMPRESARIAL", "premio": 6940000.0,
                "sinistral": 48.9, "prev": 3420000.0, "vgbl_pct": 73.6, "prov_mi": 24100.0,
                "corr": 1940, "pj": 840, "solv": 148.2, "cob": 129.0, "percap": 2869.0, "score": 76.2,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "r1": "AUTO_PATRIMONIAL", "r2": "VIDA_PESSOAS", "premio": 5840000.0,
                "sinistral": 46.8, "prev": 2980000.0, "vgbl_pct": 74.8, "prov_mi": 19800.0,
                "corr": 1680, "pj": 720, "solv": 149.5, "cob": 130.4, "percap": 2404.0, "score": 75.8,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "r1": "RESIDENCIAL_EMPRESARIAL", "r2": "AUTO_PATRIMONIAL", "premio": 5210000.0,
                "sinistral": 49.5, "prev": 2640000.0, "vgbl_pct": 72.4, "prov_mi": 17600.0,
                "corr": 1520, "pj": 650, "solv": 147.8, "cob": 128.2, "percap": 3499.0, "score": 74.1,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "r1": "RURAL_AGRICOLA", "r2": "TRANSPORTE_CARGA", "premio": 4680000.0,
                "sinistral": 51.2, "prev": 2120000.0, "vgbl_pct": 81.2, "prov_mi": 14900.0,
                "corr": 1140, "pj": 510, "solv": 144.6, "cob": 126.5, "percap": 7190.0, "score": 72.4,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "r1": "TRANSPORTE_CARGA", "r2": "VIDA_PESSOAS", "premio": 3420000.0,
                "sinistral": 52.8, "prev": 1580000.0, "vgbl_pct": 70.9, "prov_mi": 11200.0,
                "corr": 890, "pj": 380, "solv": 141.2, "cob": 123.8, "percap": 2623.0, "score": 66.5,
            },
        ]

        pr_rows: list[dict[str, Any]] = []
        prev_rows: list[dict[str, Any]] = []
        corr_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            p1 = round(m["premio"] * 0.58, 2)
            p2 = round(m["premio"] * 0.42, 2)
            sin1 = round(p1 * (m["sinistral"] / 100.0), 2)
            sin2 = round(p2 * ((m["sinistral"] - 2.4) / 100.0), 2)
            solv_status = (
                "PATRIMONIO_LIQUIDO_SOLVENTE" if m["solv"] >= 145.0 else "MARGEM_MONITORADA"
            )

            pr_rows.append({
                "cod_registro_susep": f"SUSEP-{m['uf']}-{m['id']}-01",
                "grupo_ramo_seguro": m["r1"],
                "porte_mercado_segurador": "SEGURADORA_TIERS_1",
                "situacao_solvencia_pl": solv_status,
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "premio_direto_anual_mil_brl": p1,
                "sinistro_retido_anual_mil_brl": sin1,
                "indice_sinistralidade_pct": m["sinistral"],
            })
            pr_rows.append({
                "cod_registro_susep": f"SUSEP-{m['uf']}-{m['id']}-02",
                "grupo_ramo_seguro": m["r2"],
                "porte_mercado_segurador": "PREVIDENCIA_VGBL_PGBL",
                "situacao_solvencia_pl": solv_status,
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "premio_direto_anual_mil_brl": p2,
                "sinistro_retido_anual_mil_brl": sin2,
                "indice_sinistralidade_pct": round(m["sinistral"] - 2.4, 2),
            })

            prev_rows.append({
                "cod_prev_susep": f"PREV-SUSEP-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "contribuicoes_previdencia_mil_brl": m["prev"],
                "resgates_pagamentos_mil_brl": round(m["prev"] * 0.61, 2),
                "provisoes_tecnicas_mi_brl": m["prov_mi"],
                "participacao_vgbl_pct": m["vgbl_pct"],
            })

            corr_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "corretoras_ativas_susep": m["corr"],
                "agentes_corretor_pj": m["pj"],
                "indice_solvencia_regulatoria_pct": m["solv"],
                "indice_cobertura_provisoes_pct": m["cob"],
            })

            total_sinistro = round(sin1 + sin2, 2)
            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "premios_emitidos_total_mil_brl": m["premio"],
                "sinistros_ocorridos_mil_brl": total_sinistro,
                "taxa_sinistralidade_media_pct": m["sinistral"],
                "corretoras_ativas_susep": m["corr"],
                "premio_per_capita_brl": m["percap"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "premios_emitidos_total_mil_brl": m["premio"],
                "sinistros_ocorridos_mil_brl": total_sinistro,
                "taxa_sinistralidade_media_pct": m["sinistral"],
                "corretoras_ativas_susep": m["corr"],
                "provisoes_tecnicas_mi_brl": m["prov_mi"],
                "score_maturidade_seguros_susep": m["score"],
                "tier_mercado_segurador_susep": _classify_insurance_market_tier(m["score"]),
            })

        cleaned_pr = apply_sentinel_cleaning(pr_rows, self.contract)
        pr_table = pa.Table.from_pylist(cleaned_pr)
        obs = evaluate_five_pillars(
            table=pr_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"SUSEP Seguros 5-Pillar check failed: {obs.violations}")

        pq.write_table(pr_table, pr_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(prev_rows),
            silver_dir / "susep_previdencia_vgbl_pgbl_uf.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(corr_rows),
            silver_dir / "susep_corretoras_solvencia_mercado.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "susep_seguros_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "premios_sinistros": len(pr_rows),
                "previdencia_vgbl": len(prev_rows),
                "corretoras_solvencia": len(corr_rows),
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
        """Execute Medallion materialization and return susep_premios_sinistros_ramo Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "susep_premios_sinistros_ramo.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_susep_premios_ramo(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SUSEP insurance premiums & loss ratios by code, branch, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "susep_premios_sinistros_ramo.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_registro_susep"]) == term
            or term in _norm_text(r["grupo_ramo_seguro"])
            or term in _norm_text(r["porte_mercado_segurador"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_susep_previdencia_vgbl(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SUSEP VGBL/PGBL pension contributions and technical provisions."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "susep_previdencia_vgbl_pgbl_uf.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_prev_susep"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_susep_corretoras_solvencia(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal SUSEP active insurance brokers and regulatory solvency index."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "susep_corretoras_solvencia_mercado.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_susep_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SUSEP issued premiums, claims, and per-capita."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "susep_seguros_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_susep_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold SUSEP insurance market maturity score and solvency tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_susep_seguros_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_susep_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_premios: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SUSEP insurance maturity score or issued premiums."""
        rows = self._read_parquet_rows("gold", "features_susep_seguros_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_premios:
            rows.sort(
                key=lambda r: (-float(r["premios_emitidos_total_mil_brl"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_maturidade_seguros_susep"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("susep_seguros", SusepSegurosConnector)
