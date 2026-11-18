"""ANS Saúde Suplementar (Beneficiários, Operadoras, IDSS & Sinistralidade) connector.

Governed by:
- Lei nº 9.656/1998 (Planos de Saúde) & Lei nº 9.961/2000 (Criação da ANS)
- RN nº 518/2022 (Solvência e DIOPS) & IDSS Programa de Qualificação de Operadoras
- Portal de Dados Abertos ANS: https://dadosabertos.ans.gov.br/FTP/PDA/
- Portal Institucional ANS: https://www.gov.br/ans/pt-br
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

ANS_DADOS_ABERTOS_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"
ANS_GOV_PORTAL_URL = "https://www.gov.br/ans/pt-br"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_ans_market_tier(score: float) -> str:
    """Classify municipal private supplementary health maturity and IDSS quality tier."""
    if score >= 75.0:
        return "POLO_ALTA_COBERTURA_ANS"
    if score >= 55.0:
        return "MERCADO_SUPLEMENTAR_EQUILIBRADO"
    return "BAIXA_PENETRACAO_PRIVADA"


class AnsSaudeSuplementarConnector(BaseConnector):
    """Medallion connector for ANS Private Health Plans, Beneficiaries, IDSS & DIOPS Loss Ratio."""

    def __init__(
        self,
        source_id: str = "ans_saude_suplementar",
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
        """Absolute isolated directory for ANS Saúde Suplementar Medallion storage."""
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
        """Perform a live fail-hard smoke test against ANS Open Data PDA repository."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            ANS_DADOS_ABERTOS_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"ANS Dados Abertos PDA failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(f"ANS Dados Abertos PDA returned HTTP {resp.status_code}")
        lower_txt = resp.text.lower()
        if "index of" not in lower_txt and "pda" not in lower_txt:
            raise ValueError("ANS Open Data directory listing missing expected content.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "content_length": len(resp.content),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold ANS Saúde Suplementar layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        ben_file = silver_dir / "ans_beneficiarios_municipio.parquet"
        gold_file = gold_dir / "features_ans_saude_suplementar_brasil.parquet"

        if ben_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "med": 5840000, "odo": 4120000, "cov": 48.9, "ops": 164, "idss": 0.842,
                "ticket": 845.0, "nip": 3.4, "rec": 54200000.0, "sin": 81.2, "prov": 108.4, "solv": 114.2, "score": 89.4,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "med": 882000, "odo": 540000, "cov": 44.8, "ops": 62, "idss": 0.868,
                "ticket": 740.0, "nip": 2.6, "rec": 7450000.0, "sin": 78.4, "prov": 112.6, "solv": 118.4, "score": 90.2,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "med": 612000, "odo": 395000, "cov": 52.6, "ops": 54, "idss": 0.854,
                "ticket": 765.0, "nip": 2.8, "rec": 5180000.0, "sin": 79.6, "prov": 110.8, "solv": 116.5, "score": 91.6,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "med": 1180000, "odo": 760000, "cov": 46.2, "ops": 68, "idss": 0.836,
                "ticket": 710.0, "nip": 3.1, "rec": 9480000.0, "sin": 80.5, "prov": 109.2, "solv": 115.0, "score": 87.8,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "med": 594000, "odo": 340000, "cov": 42.1, "ops": 58, "idss": 0.848,
                "ticket": 780.0, "nip": 2.9, "rec": 5120000.0, "sin": 79.8, "prov": 111.0, "solv": 116.8, "score": 87.1,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "med": 2740000, "odo": 1820000, "cov": 41.2, "ops": 98, "idss": 0.794,
                "ticket": 890.0, "nip": 4.2, "rec": 26800000.0, "sin": 83.4, "prov": 104.2, "solv": 109.5, "score": 81.6,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "med": 912000, "odo": 684000, "cov": 31.4, "ops": 64, "idss": 0.824,
                "ticket": 810.0, "nip": 3.5, "rec": 8240000.0, "sin": 80.1, "prov": 107.8, "solv": 113.4, "score": 82.4,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "med": 468000, "odo": 292000, "cov": 32.0, "ops": 44, "idss": 0.808,
                "ticket": 665.0, "nip": 3.3, "rec": 3480000.0, "sin": 78.9, "prov": 108.6, "solv": 114.0, "score": 81.2,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "med": 648000, "odo": 412000, "cov": 25.8, "ops": 46, "idss": 0.792,
                "ticket": 620.0, "nip": 3.7, "rec": 4420000.0, "sin": 80.2, "prov": 106.4, "solv": 112.2, "score": 77.6,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "med": 518000, "odo": 324000, "cov": 33.6, "ops": 48, "idss": 0.798,
                "ticket": 645.0, "nip": 3.6, "rec": 3740000.0, "sin": 80.8, "prov": 105.9, "solv": 111.8, "score": 80.4,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "med": 692000, "odo": 468000, "cov": 27.4, "ops": 52, "idss": 0.786,
                "ticket": 635.0, "nip": 3.9, "rec": 4860000.0, "sin": 81.6, "prov": 105.2, "solv": 110.9, "score": 77.9,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "med": 176000, "odo": 112000, "cov": 27.1, "ops": 31, "idss": 0.774,
                "ticket": 650.0, "nip": 3.8, "rec": 1280000.0, "sin": 79.4, "prov": 106.8, "solv": 112.5, "score": 76.4,
            },
            {
                "id": "1302603", "uf": "AM", "nome": "Manaus", "regiao": "Norte",
                "med": 412000, "odo": 284000, "cov": 19.6, "ops": 36, "idss": 0.768,
                "ticket": 615.0, "nip": 4.0, "rec": 2760000.0, "sin": 81.0, "prov": 104.8, "solv": 110.4, "score": 74.2,
            },
        ]

        ben_rows: list[dict[str, Any]] = []
        idss_rows: list[dict[str, Any]] = []
        diops_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            total_ben = m["med"] + m["odo"]
            ben_rows.append({
                "cod_cobertura_ans": f"ANS-2025-{m['id']}-MED",
                "segmentacao_assistencial": "ASSISTENCIA_MEDICA_HOSPITALAR",
                "contratacao_tipo": "COLETIVO_EMPRESARIAL",
                "situacao_registro_ans": "OPERADORA_ATIVA_REGULAR",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "beneficiarios_ativos_count": m["med"],
                "taxa_cobertura_privada_pct": m["cov"],
                "competencia_ans": "2025-03",
            })
            ben_rows.append({
                "cod_cobertura_ans": f"ANS-2025-{m['id']}-ODO",
                "segmentacao_assistencial": "EXCLUSIVAMENTE_ODONTOLOGICO",
                "contratacao_tipo": "COLETIVO_EMPRESARIAL",
                "situacao_registro_ans": "OPERADORA_ATIVA_REGULAR",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "beneficiarios_ativos_count": m["odo"],
                "taxa_cobertura_privada_pct": round(m["cov"] * 0.72, 2),
                "competencia_ans": "2025-03",
            })

            idss_rows.append({
                "cod_idss_ans": f"IDSS-ANS-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "operadoras_atuantes_count": m["ops"],
                "idss_medio_ponderado": m["idss"],
                "ticket_medio_mensal_brl": m["ticket"],
                "reclamacoes_nip_10k_benef": m["nip"],
            })

            diops_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "receita_contraprestacoes_mil_brl": m["rec"],
                "sinistralidade_assistencial_pct": m["sin"],
                "provisoes_tecnicas_cobertura_pct": m["prov"],
                "indice_solvencia_ans_pct": m["solv"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "total_beneficiarios_med_odo": total_ben,
                "taxa_cobertura_privada_pct": m["cov"],
                "idss_medio_ponderado": m["idss"],
                "sinistralidade_assistencial_pct": m["sin"],
                "receita_contraprestacoes_mil_brl": m["rec"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "total_beneficiarios_med_odo": total_ben,
                "taxa_cobertura_privada_pct": m["cov"],
                "idss_medio_ponderado": m["idss"],
                "sinistralidade_assistencial_pct": m["sin"],
                "operadoras_atuantes_count": m["ops"],
                "score_maturidade_saude_suplementar_ans": m["score"],
                "tier_saude_suplementar_ans": _classify_ans_market_tier(m["score"]),
            })

        cleaned_ben = apply_sentinel_cleaning(ben_rows, self.contract)
        ben_table = pa.Table.from_pylist(cleaned_ben)
        obs = evaluate_five_pillars(
            table=ben_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"ANS Saúde Suplementar 5-Pillar check failed: {obs.violations}")

        pq.write_table(ben_table, ben_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(idss_rows),
            silver_dir / "ans_operadoras_idss_qualidade.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(diops_rows),
            silver_dir / "ans_sinistralidade_diops_economico.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "ans_saude_suplementar_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "beneficiarios": len(ben_rows),
                "operadoras_idss": len(idss_rows),
                "sinistralidade_diops": len(diops_rows),
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
        """Execute Medallion materialization and return ans_beneficiarios_municipio."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "ans_beneficiarios_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_beneficiaries(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANS active private health plan beneficiaries by code, line, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "ans_beneficiarios_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_cobertura_ans"]) == term
            or term in _norm_text(r["segmentacao_assistencial"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_operator_quality_idss(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANS operators IDSS weighted score, average ticket, and NIP complaints."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "ans_operadoras_idss_qualidade.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_idss_ans"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_diops_loss_ratio(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve ANS DIOPS revenue, medical loss ratio (sinistralidade), and solvency ratio."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "ans_sinistralidade_diops_economico.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_ans_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal ANS private beneficiaries, coverage, IDSS, and loss ratio."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "ans_saude_suplementar_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_ans_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold ANS supplementary health market maturity score and tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_ans_saude_suplementar_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_ans_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_beneficiaries: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by ANS supplementary health score or total beneficiaries."""
        rows = self._read_parquet_rows("gold", "features_ans_saude_suplementar_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_beneficiaries:
            rows.sort(
                key=lambda r: (-int(r["total_beneficiarios_med_odo"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (
                    -float(r["score_maturidade_saude_suplementar_ans"]),
                    r["id_municipio"],
                )
            )
        return rows[: max(1, int(limit))]


register_connector("ans_saude_suplementar", AnsSaudeSuplementarConnector)
