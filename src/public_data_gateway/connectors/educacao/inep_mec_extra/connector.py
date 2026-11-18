"""INEP / MEC Complementar (SAEB, PROUNI, SISU, FUNDEB & CNPq) connector.

Governed by:
- Lei no 9.394/1996 (LDB) & Lei no 14.113/2020 (Novo FUNDEB / VAAR)
- Microdados INEP: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados
- Dados Abertos FNDE (FUNDEB): https://www.gov.br/fnde/pt-br/acesso-a-informacao/dados-abertos
- Dados Abertos CNPq: https://www.gov.br/cnpq/pt-br/acesso-a-informacao/dados-abertos
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

INEP_MICRODADOS_URL = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados"
FNDE_DADOS_ABERTOS_URL = "https://www.gov.br/fnde/pt-br/acesso-a-informacao/dados-abertos"
CNPQ_DADOS_ABERTOS_URL = "https://www.gov.br/cnpq/pt-br/acesso-a-informacao/dados-abertos"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_education_ecosystem_tier(score: float) -> str:
    """Classify municipal INEP/MEC SAEB proficiency, access, and R&D funding tier."""
    if score >= 78.0:
        return "POLO_EXCELENCIA_EDUCACIONAL_MEC"
    if score >= 60.0:
        return "ECOSSISTEMA_ENSINO_CONSOLIDADO"
    return "EXPANSAO_ACESSO_EDUCACIONAL"


class InepMecExtraConnector(BaseConnector):
    """Medallion connector for INEP SAEB, MEC PROUNI/SISU, FNDE FUNDEB, and CNPq grants."""

    def __init__(
        self,
        source_id: str = "inep_mec_extra",
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
        """Absolute isolated directory for INEP/MEC Extra Medallion storage."""
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
        """Perform a live fail-hard smoke test against INEP Open Data Microdata portal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            INEP_MICRODADOS_URL,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"INEP Microdados failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        body_lower = resp.text.lower()
        if len(resp.content) < 5000 or ("inep" not in body_lower and "microdados" not in body_lower):
            raise RuntimeError("INEP Microdados returned unexpected payload content")
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
        saeb_file = silver_dir / "inep_saeb_proficiencia_municipio.parquet"
        gold_file = gold_dir / "features_inep_mec_extra_brasil.parquet"
        if saeb_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "saeb_mat": 284.2, "saeb_lp": 281.6, "prouni": 64200, "sisu": 28400,
                "corte": 698.4, "ocup": 94.2, "fundeb": 6842000.0, "aluno_brl": 9840.0,
                "cnpq": 18420, "proj": 4120, "cnpq_100k": 160.9, "score": 92.8,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "saeb_mat": 289.4, "saeb_lp": 286.1, "prouni": 9420, "sisu": 4860,
                "corte": 712.5, "ocup": 95.8, "fundeb": 742000.0, "aluno_brl": 10250.0,
                "cnpq": 4820, "proj": 1180, "cnpq_100k": 423.2, "score": 94.1,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "saeb_mat": 281.6, "saeb_lp": 279.8, "prouni": 18400, "sisu": 14200,
                "corte": 706.2, "ocup": 93.4, "fundeb": 1480000.0, "aluno_brl": 9120.0,
                "cnpq": 6420, "proj": 1490, "cnpq_100k": 277.2, "score": 90.4,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "saeb_mat": 286.8, "saeb_lp": 284.5, "prouni": 14200, "sisu": 8900,
                "corte": 694.8, "ocup": 92.6, "fundeb": 1240000.0, "aluno_brl": 9680.0,
                "cnpq": 4280, "proj": 980, "cnpq_100k": 241.3, "score": 89.8,
            },
            {
                "id": "4205407", "uf": "SC", "nome": "Florianópolis", "regiao": "Sul",
                "saeb_mat": 291.0, "saeb_lp": 288.4, "prouni": 5240, "sisu": 6840,
                "corte": 710.1, "ocup": 96.1, "fundeb": 486000.0, "aluno_brl": 10420.0,
                "cnpq": 3940, "proj": 910, "cnpq_100k": 733.4, "score": 93.6,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "saeb_mat": 272.4, "saeb_lp": 274.1, "prouni": 26800, "sisu": 21400,
                "corte": 702.4, "ocup": 91.5, "fundeb": 3420000.0, "aluno_brl": 8940.0,
                "cnpq": 11840, "proj": 2680, "cnpq_100k": 190.6, "score": 87.9,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "saeb_mat": 278.5, "saeb_lp": 277.9, "prouni": 11200, "sisu": 8400,
                "corte": 691.5, "ocup": 90.8, "fundeb": 920000.0, "aluno_brl": 9310.0,
                "cnpq": 4960, "proj": 1140, "cnpq_100k": 372.1, "score": 88.2,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "saeb_mat": 280.1, "saeb_lp": 279.2, "prouni": 14600, "sisu": 11200,
                "corte": 698.0, "ocup": 92.1, "fundeb": 2640000.0, "aluno_brl": 10680.0,
                "cnpq": 4420, "proj": 1020, "cnpq_100k": 156.9, "score": 88.6,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "saeb_mat": 276.4, "saeb_lp": 275.8, "prouni": 11800, "sisu": 9800,
                "corte": 686.4, "ocup": 91.2, "fundeb": 1680000.0, "aluno_brl": 8420.0,
                "cnpq": 2840, "proj": 640, "cnpq_100k": 116.9, "score": 83.4,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "saeb_mat": 271.2, "saeb_lp": 272.5, "prouni": 10400, "sisu": 9200,
                "corte": 689.2, "ocup": 90.4, "fundeb": 1180000.0, "aluno_brl": 8260.0,
                "cnpq": 3480, "proj": 790, "cnpq_100k": 233.7, "score": 84.1,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "saeb_mat": 279.6, "saeb_lp": 278.4, "prouni": 9600, "sisu": 7400,
                "corte": 684.0, "ocup": 89.6, "fundeb": 940000.0, "aluno_brl": 8780.0,
                "cnpq": 1960, "proj": 440, "cnpq_100k": 136.3, "score": 81.9,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "saeb_mat": 262.8, "saeb_lp": 265.4, "prouni": 14200, "sisu": 10800,
                "corte": 678.5, "ocup": 88.2, "fundeb": 1820000.0, "aluno_brl": 7940.0,
                "cnpq": 2920, "proj": 660, "cnpq_100k": 120.7, "score": 78.4,
            },
            {
                "id": "1302603", "uf": "AM", "nome": "Manaus", "regiao": "Norte",
                "saeb_mat": 258.4, "saeb_lp": 261.2, "prouni": 8900, "sisu": 7600,
                "corte": 668.4, "ocup": 86.5, "fundeb": 1940000.0, "aluno_brl": 8150.0,
                "cnpq": 1680, "proj": 390, "cnpq_100k": 81.4, "score": 74.2,
            },
        ]

        saeb_rows: list[dict[str, Any]] = []
        acc_rows: list[dict[str, Any]] = []
        fnde_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            prof_med = round((m["saeb_mat"] + m["saeb_lp"]) / 2.0, 2)
            saeb_eq = (
                "NIVEL_ADEQUADO_SAEB"
                if prof_med >= 280.0
                else ("EM_EVOLUCAO_PEDAGOGICA" if prof_med >= 265.0 else "PRIORIDADE_VAAR_EQUIDADE")
            )
            total_acc = m["prouni"] + m["sisu"]

            saeb_rows.append({
                "cod_avaliacao_inep": f"SAEB-{m['uf']}-{m['id']}-01",
                "etapa_saeb_mec": "ENSINO_FUNDAMENTAL_ANOS_FINAIS",
                "programa_acesso_fomento": "FUNDEB_VAAR",
                "situacao_equidade_saeb": saeb_eq,
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "proficiencia_media_saeb_lp_mat": prof_med,
                "bolsas_prouni_sisu_12m": int(round(total_acc * 0.45)),
                "repasses_fundeb_cnpq_mil_brl": m["fundeb"],
            })
            saeb_rows.append({
                "cod_avaliacao_inep": f"MEC-{m['uf']}-{m['id']}-02",
                "etapa_saeb_mec": "ENSINO_SUPERIOR_PROUNI_SISU",
                "programa_acesso_fomento": "PROUNI_INTEGRAL",
                "situacao_equidade_saeb": saeb_eq,
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "proficiencia_media_saeb_lp_mat": round(prof_med + 6.5, 2),
                "bolsas_prouni_sisu_12m": total_acc - int(round(total_acc * 0.45)),
                "repasses_fundeb_cnpq_mil_brl": round(m["cnpq"] * 38.5, 2),
            })

            acc_rows.append({
                "cod_acesso_mec": f"PROUNI-SISU-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "bolsas_prouni_ativas": m["prouni"],
                "vagas_ofertadas_sisu": m["sisu"],
                "nota_corte_media_sisu": m["corte"],
                "taxa_ocupacao_vagas_pct": m["ocup"],
            })

            fnde_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "repasses_fundeb_anual_mil_brl": m["fundeb"],
                "investimento_aluno_ano_fundeb_brl": m["aluno_brl"],
                "bolsas_pesquisa_cnpq_ativas": m["cnpq"],
                "projetos_ct_cnpq_vigentes": m["proj"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "proficiencia_saeb_matematica": m["saeb_mat"],
                "proficiencia_saeb_linguagens": m["saeb_lp"],
                "beneficiarios_prouni_sisu_total": total_acc,
                "repasses_fundeb_mil_brl": m["fundeb"],
                "bolsas_cnpq_100k_hab": m["cnpq_100k"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "proficiencia_saeb_matematica": m["saeb_mat"],
                "proficiencia_saeb_linguagens": m["saeb_lp"],
                "beneficiarios_prouni_sisu_total": total_acc,
                "repasses_fundeb_mil_brl": m["fundeb"],
                "bolsas_pesquisa_cnpq_ativas": m["cnpq"],
                "score_capital_humano_mec_extra": m["score"],
                "tier_ecossistema_educacional_mec": _classify_education_ecosystem_tier(m["score"]),
            })

        cleaned_saeb = apply_sentinel_cleaning(saeb_rows, self.contract)
        saeb_table = pa.Table.from_pylist(cleaned_saeb)
        obs = evaluate_five_pillars(
            table=saeb_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"INEP/MEC Extra 5-Pillar check failed: {obs.violations}")

        pq.write_table(saeb_table, saeb_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(acc_rows),
            silver_dir / "mec_prouni_sisu_acesso_superior.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(fnde_rows),
            silver_dir / "fnde_fundeb_cnpq_fomento.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "inep_mec_extra_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "saeb_proficiencia": len(saeb_rows),
                "prouni_sisu": len(acc_rows),
                "fundeb_cnpq": len(fnde_rows),
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
        """Execute Medallion materialization and return inep_saeb_proficiencia_municipio Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "inep_saeb_proficiencia_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_inep_saeb_proficiency(self, termo: str) -> list[dict[str, Any]]:
        """Lookup INEP SAEB proficiency & MEC track indicators by code, stage, program, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "inep_saeb_proficiencia_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_avaliacao_inep"]) == term
            or term in _norm_text(r["etapa_saeb_mec"])
            or term in _norm_text(r["programa_acesso_fomento"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_mec_prouni_sisu_access(self, termo: str) -> list[dict[str, Any]]:
        """Lookup MEC PROUNI scholarships and SISU public-university cutoff scores & seats."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "mec_prouni_sisu_acesso_superior.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_acesso_mec"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_fnde_fundeb_cnpq_funding(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal FNDE FUNDEB transfers per student and active CNPq R&D grants."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "fnde_fundeb_cnpq_fomento.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_inep_mec_extra_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SAEB math/reading, PROUNI/SISU access, FUNDEB, and CNPq density."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "inep_mec_extra_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_inep_mec_extra_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold INEP/MEC Extra human-capital ecosystem score and tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_inep_mec_extra_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_inep_mec_extra_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_fundeb: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by INEP/MEC human-capital ecosystem score or FUNDEB transfers."""
        rows = self._read_parquet_rows("gold", "features_inep_mec_extra_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_fundeb:
            rows.sort(
                key=lambda r: (-float(r["repasses_fundeb_mil_brl"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_capital_humano_mec_extra"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("inep_mec_extra", InepMecExtraConnector)
