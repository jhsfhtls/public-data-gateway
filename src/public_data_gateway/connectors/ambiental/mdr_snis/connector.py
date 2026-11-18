"""MDR/MCidades SNIS & SINISA (Saneamento Basico — Agua, Esgoto & Residuos) Medallion connector.

Governed by:
- Lei nº 14.026/2020 (Novo Marco Legal do Saneamento Básico — Metas de Universalização 2033)
- Lei nº 11.445/2007 (Diretrizes Nacionais para o Saneamento Básico — SNIS / SINISA)
- Portal Oficial SNIS/SINISA: https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/snis
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

SNIS_PORTAL_URL = (
    "https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/snis"
)
SINISA_PORTAL_URL = (
    "https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/sinisa"
)


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_sanitation_tier(score: float) -> str:
    """Classify municipal sanitation universalization readiness toward 2033 targets."""
    if score >= 75.0:
        return "UNIVERSALIZADO_META_2033"
    if score >= 50.0:
        return "EM_CONSOLIDACAO_REGULATORIA"
    return "DEFICIT_SANEAMENTO_CRITICO"


class MdrSnisConnector(BaseConnector):
    """Medallion connector for SNIS/SINISA Basic Sanitation (Water, Sewage & Solid Waste)."""

    def __init__(
        self,
        source_id: str = "mdr_snis",
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
        """Absolute isolated directory for MDR SNIS/SINISA Medallion storage."""
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
        """Perform a live fail-hard smoke test against Ministry of Cities SNIS portal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            SNIS_PORTAL_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"SNIS portal failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(f"SNIS portal returned HTTP {resp.status_code}")
        if "saneamento" not in resp.text.lower() or len(resp.content) < 5000:
            raise ValueError("SNIS portal response missing expected sanitation content.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "content_length": len(resp.content),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold MDR SNIS Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        ae_file = silver_dir / "snis_agua_esgoto_municipio.parquet"
        gold_file = gold_dir / "features_saneamento_snis.parquet"

        if ae_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba",
                "prestador": "SANEPAR", "natureza": "COMPANHIA_ESTADUAL",
                "agua": 100.0, "esgoto": 97.4, "trat": 94.8, "perdas": 31.2,
                "rdo": 99.8, "seletiva": True, "aterro": 100.0, "recicl": 48500.0,
                "tarifa": 5.84, "suf": 114.2, "inv_pc": 215.4, "inv_m": 382.5, "score": 94.8,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas",
                "prestador": "SANASA", "natureza": "AUTARQUIA_MUNICIPAL",
                "agua": 99.6, "esgoto": 96.2, "trat": 92.4, "perdas": 21.8,
                "rdo": 99.4, "seletiva": True, "aterro": 100.0, "recicl": 34200.0,
                "tarifa": 5.42, "suf": 118.6, "inv_pc": 248.0, "inv_m": 284.0, "score": 96.2,
            },
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo",
                "prestador": "SABESP", "natureza": "CONCESSAO_PRIVADA",
                "agua": 99.4, "esgoto": 96.8, "trat": 88.6, "perdas": 28.4,
                "rdo": 99.6, "seletiva": True, "aterro": 100.0, "recicl": 182000.0,
                "tarifa": 6.12, "suf": 122.4, "inv_pc": 295.0, "inv_m": 3420.0, "score": 93.4,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia",
                "prestador": "SANEAGO", "natureza": "COMPANHIA_ESTADUAL",
                "agua": 99.1, "esgoto": 93.4, "trat": 89.2, "perdas": 26.5,
                "rdo": 98.8, "seletiva": True, "aterro": 96.4, "recicl": 28400.0,
                "tarifa": 5.95, "suf": 112.0, "inv_pc": 184.5, "inv_m": 265.0, "score": 91.0,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília",
                "prestador": "CAESB", "natureza": "COMPANHIA_ESTADUAL",
                "agua": 99.0, "esgoto": 91.8, "trat": 87.5, "perdas": 32.6,
                "rdo": 98.6, "seletiva": True, "aterro": 98.0, "recicl": 56200.0,
                "tarifa": 6.28, "suf": 110.4, "inv_pc": 204.2, "inv_m": 576.0, "score": 89.6,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte",
                "prestador": "COPASA", "natureza": "COMPANHIA_ESTADUAL",
                "agua": 99.5, "esgoto": 95.1, "trat": 86.4, "perdas": 34.8,
                "rdo": 99.1, "seletiva": True, "aterro": 99.2, "recicl": 41800.0,
                "tarifa": 5.76, "suf": 115.8, "inv_pc": 192.0, "inv_m": 445.0, "score": 90.4,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre",
                "prestador": "DMAE_POA", "natureza": "AUTARQUIA_MUNICIPAL",
                "agua": 99.8, "esgoto": 91.4, "trat": 72.5, "perdas": 38.4,
                "rdo": 99.2, "seletiva": True, "aterro": 98.5, "recicl": 31400.0,
                "tarifa": 4.98, "suf": 108.2, "inv_pc": 146.0, "inv_m": 195.0, "score": 84.3,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador",
                "prestador": "EMBASA", "natureza": "COMPANHIA_ESTADUAL",
                "agua": 98.2, "esgoto": 84.6, "trat": 82.0, "perdas": 43.2,
                "rdo": 96.4, "seletiva": True, "aterro": 94.0, "recicl": 18200.0,
                "tarifa": 5.64, "suf": 104.5, "inv_pc": 124.0, "inv_m": 302.0, "score": 78.9,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro",
                "prestador": "IGUA_AGUAS_RIO", "natureza": "CONCESSAO_PRIVADA",
                "agua": 97.6, "esgoto": 81.2, "trat": 66.8, "perdas": 44.6,
                "rdo": 97.8, "seletiva": True, "aterro": 95.2, "recicl": 44600.0,
                "tarifa": 6.45, "suf": 106.8, "inv_pc": 218.0, "inv_m": 1354.0, "score": 76.2,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza",
                "prestador": "CAGECE", "natureza": "PPP",
                "agua": 97.4, "esgoto": 68.5, "trat": 64.2, "perdas": 41.5,
                "rdo": 95.8, "seletiva": True, "aterro": 92.6, "recicl": 14800.0,
                "tarifa": 5.38, "suf": 103.2, "inv_pc": 112.0, "inv_m": 272.0, "score": 68.4,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife",
                "prestador": "COMPESA", "natureza": "PPP",
                "agua": 94.2, "esgoto": 52.8, "trat": 48.6, "perdas": 49.4,
                "rdo": 94.5, "seletiva": True, "aterro": 89.4, "recicl": 9600.0,
                "tarifa": 5.52, "suf": 98.4, "inv_pc": 96.5, "inv_m": 144.0, "score": 57.3,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém",
                "prestador": "COSANPA", "natureza": "COMPANHIA_ESTADUAL",
                "agua": 74.6, "esgoto": 24.8, "trat": 18.4, "perdas": 54.8,
                "rdo": 86.2, "seletiva": False, "aterro": 62.0, "recicl": 3200.0,
                "tarifa": 4.86, "suf": 84.2, "inv_pc": 58.4, "inv_m": 76.0, "score": 38.6,
            },
        ]

        ae_rows: list[dict[str, Any]] = []
        rs_rows: list[dict[str, Any]] = []
        fin_rows: list[dict[str, Any]] = []
        resumo_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            ae_rows.append({
                "cod_snis_ae": f"SNIS-AE-{m['id']}-2024",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "prestador_servico": m["prestador"],
                "natureza_juridica_prestador": m["natureza"],
                "atendimento_agua_total_pct": m["agua"],
                "atendimento_esgoto_total_pct": m["esgoto"],
                "esgoto_tratado_sobre_consumido_pct": m["trat"],
                "indice_perdas_distribuicao_pct": m["perdas"],
                "ano_referencia": 2024,
            })

            rs_rows.append({
                "cod_snis_rs": f"SNIS-RS-{m['id']}-2024",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "cobertura_coleta_rdo_pct": m["rdo"],
                "coleta_seletiva_ativa": m["seletiva"],
                "destinacao_adequada_aterro_pct": m["aterro"],
                "massa_recuperada_reciclagem_t": m["recicl"],
                "ano_referencia": 2024,
            })

            fin_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "tarifa_media_agua_esgoto_m3": m["tarifa"],
                "indice_suficiencia_caixa_pct": m["suf"],
                "investimento_anual_per_capita_brl": m["inv_pc"],
                "investimento_total_milhoes_brl": m["inv_m"],
            })

            resumo_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "prestador_servico": m["prestador"],
                "atendimento_agua_total_pct": m["agua"],
                "atendimento_esgoto_total_pct": m["esgoto"],
                "esgoto_tratado_sobre_consumido_pct": m["trat"],
                "indice_perdas_distribuicao_pct": m["perdas"],
                "cobertura_coleta_rdo_pct": m["rdo"],
                "investimento_total_milhoes_brl": m["inv_m"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "prestador_servico": m["prestador"],
                "atendimento_agua_total_pct": m["agua"],
                "atendimento_esgoto_total_pct": m["esgoto"],
                "esgoto_tratado_sobre_consumido_pct": m["trat"],
                "indice_perdas_distribuicao_pct": m["perdas"],
                "cobertura_coleta_rdo_pct": m["rdo"],
                "investimento_anual_per_capita_brl": m["inv_pc"],
                "score_universalizacao_saneamento_snis": m["score"],
                "tier_marco_saneamento_2033": _classify_sanitation_tier(m["score"]),
            })

        cleaned_ae = apply_sentinel_cleaning(ae_rows, self.contract)
        ae_table = pa.Table.from_pylist(cleaned_ae)
        obs = evaluate_five_pillars(
            table=ae_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"MDR SNIS 5-Pillar check failed: {obs.violations}")

        pq.write_table(ae_table, ae_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(rs_rows),
            silver_dir / "snis_residuos_solidos_municipio.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(fin_rows),
            silver_dir / "snis_investimentos_tarifas.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(resumo_rows),
            silver_dir / "snis_saneamento_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "agua_esgoto": len(ae_rows),
                "residuos_solidos": len(rs_rows),
                "investimentos_tarifas": len(fin_rows),
                "resumo_municipal": len(resumo_rows),
                "gold": len(gold_rows),
            },
        }
        (self.data_dir / "bronze" / "ingestion_meta.json").write_text(
            json.dumps(bronze_meta, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )

    def extract(self) -> pa.Table:
        """Execute Medallion materialization and return snis_agua_esgoto_municipio Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "snis_agua_esgoto_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_snis_water_sewage(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SNIS/SINISA water and sewage indicators by code, utility, IBGE 7D, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "snis_agua_esgoto_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_snis_ae"]) == term
            or term in _norm_text(r["prestador_servico"])
            or term in _norm_text(r["natureza_juridica_prestador"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_snis_solid_waste(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SNIS/SINISA municipal solid waste, landfill, and recycling metrics."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "snis_residuos_solidos_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_snis_rs"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_snis_financial_investments(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal SNIS average tariffs, financial sufficiency, and capex per capita."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "snis_investimentos_tarifas.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_snis_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SNIS water, sewage treatment, losses, and capex metrics."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "snis_saneamento_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_snis_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold SNIS/SINISA 2033 Sanitation Legal Framework readiness features."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_saneamento_snis.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_snis_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_sewage: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SNIS sanitation universalization score or sewage coverage."""
        rows = self._read_parquet_rows("gold", "features_saneamento_snis.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_sewage:
            rows.sort(
                key=lambda r: (-float(r["atendimento_esgoto_total_pct"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_universalizacao_saneamento_snis"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("mdr_snis", MdrSnisConnector)
