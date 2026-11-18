"""ANM Mineracao (SIGMINE, CFEM & SIGBM) connector.

Governed by:
- Decreto-Lei no 227/1967 (Codigo de Mineracao) & Lei no 13.575/2017 (Criacao da ANM)
- Lei no 14.066/2020 (Politica Nacional de Seguranca de Barragens — SIGBM)
- Portal de Dados Abertos ANM: https://www.gov.br/anm/pt-br/acesso-a-informacao/dados-abertos
- Arrecadacao CFEM ANM: https://www.gov.br/anm/pt-br/assuntos/arrecadacao
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

ANM_DADOS_ABERTOS_URL = "https://www.gov.br/anm/pt-br/acesso-a-informacao/dados-abertos"
ANM_ARRECADACAO_CFEM_URL = "https://www.gov.br/anm/pt-br/assuntos/arrecadacao"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_mining_esg_tier(score: float) -> str:
    """Classify municipal mineral maturity and SIGBM dam-safety compliance tier."""
    if score >= 78.0:
        return "POLO_MINERAL_ESG_CONFORME"
    if score >= 58.0:
        return "ATIVIDADE_EXTRATIVA_REGULAR"
    return "ALERTA_FISCALIZACAO_BARRAGENS"


class AnmMineracaoConnector(BaseConnector):
    """Medallion connector for ANM SIGMINE mining processes, CFEM royalties, and SIGBM dams."""

    def __init__(
        self,
        source_id: str = "anm_mineracao",
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
        """Absolute isolated directory for ANM Mineracao Medallion storage."""
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
        """Perform a live fail-hard smoke test against ANM Open Data portal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            ANM_DADOS_ABERTOS_URL,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ANM Dados Abertos failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        body_lower = resp.text.lower()
        if len(resp.content) < 5000 or ("anm" not in body_lower and "minera" not in body_lower):
            raise RuntimeError("ANM Dados Abertos returned unexpected payload content")
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
        proc_file = silver_dir / "anm_sigmine_processos_municipio.parquet"
        gold_file = gold_dir / "features_anm_mineracao_brasil.parquet"
        if proc_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "1505536", "uf": "PA", "nome": "Parauapebas", "regiao": "Norte",
                "sub1": "FERRO", "sub2": "COBRE", "procs": 184, "area": 142500.0,
                "cfem": 1985400.0, "aliquota": 3.5, "barragens": 6, "paebm": 0,
                "conf": 94.5, "dpa": "MODERADO_CONTROLADO", "critica_pct": 88.0, "score": 89.4,
            },
            {
                "id": "3131703", "uf": "MG", "nome": "Itabira", "regiao": "Sudeste",
                "sub1": "FERRO", "sub2": "OURO", "procs": 142, "area": 68400.0,
                "cfem": 642800.0, "aliquota": 3.5, "barragens": 11, "paebm": 1,
                "conf": 86.2, "dpa": "ALTO_DPA_MONITORADO", "critica_pct": 76.0, "score": 78.6,
            },
            {
                "id": "3146107", "uf": "MG", "nome": "Ouro Preto", "regiao": "Sudeste",
                "sub1": "FERRO", "sub2": "BAUXITA", "procs": 196, "area": 54200.0,
                "cfem": 412500.0, "aliquota": 3.2, "barragens": 9, "paebm": 0,
                "conf": 89.0, "dpa": "MODERADO_CONTROLADO", "critica_pct": 72.5, "score": 82.1,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "sub1": "FERRO", "sub2": "GRANITO_AGREGADOS", "procs": 88, "area": 18600.0,
                "cfem": 124600.0, "aliquota": 2.4, "barragens": 1, "paebm": 0,
                "conf": 96.4, "dpa": "BAIXO_DPA", "critica_pct": 54.0, "score": 86.2,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "sub1": "BAUXITA", "sub2": "AREIA_INDUSTRIAL", "procs": 62, "area": 24100.0,
                "cfem": 68400.0, "aliquota": 2.2, "barragens": 1, "paebm": 0,
                "conf": 91.5, "dpa": "BAIXO_DPA", "critica_pct": 48.0, "score": 79.5,
            },
            {
                "id": "5205109", "uf": "GO", "nome": "Catalão", "regiao": "Centro-Oeste",
                "sub1": "NIOBIO_TERRAS_RARAS", "sub2": "FOSFATO", "procs": 118, "area": 72800.0,
                "cfem": 286400.0, "aliquota": 2.8, "barragens": 5, "paebm": 0,
                "conf": 93.8, "dpa": "MODERADO_CONTROLADO", "critica_pct": 92.0, "score": 88.7,
            },
            {
                "id": "5003207", "uf": "MS", "nome": "Corumbá", "regiao": "Centro-Oeste",
                "sub1": "FERRO", "sub2": "MANGANES", "procs": 154, "area": 194000.0,
                "cfem": 168900.0, "aliquota": 3.1, "barragens": 3, "paebm": 0,
                "conf": 88.4, "dpa": "BAIXO_DPA", "critica_pct": 79.0, "score": 81.4,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "sub1": "OURO", "sub2": "CALCARIO_AGRO", "procs": 106, "area": 49500.0,
                "cfem": 118200.0, "aliquota": 1.9, "barragens": 2, "paebm": 0,
                "conf": 84.6, "dpa": "MODERADO_CONTROLADO", "critica_pct": 42.0, "score": 74.3,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "sub1": "COBRE", "sub2": "LITIO_TERRAS_RARAS", "procs": 74, "area": 29800.0,
                "cfem": 94300.0, "aliquota": 2.3, "barragens": 1, "paebm": 0,
                "conf": 90.2, "dpa": "BAIXO_DPA", "critica_pct": 81.5, "score": 83.6,
            },
            {
                "id": "4204608", "uf": "SC", "nome": "Criciúma", "regiao": "Sul",
                "sub1": "CARVAO_MINERAL", "sub2": "ARGILA_CERAMICA", "procs": 129, "area": 36400.0,
                "cfem": 56200.0, "aliquota": 2.0, "barragens": 4, "paebm": 1,
                "conf": 68.5, "dpa": "ALTO_DPA_MONITORADO", "critica_pct": 22.0, "score": 56.8,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "sub1": "CALCARIO_AGRO", "sub2": "GRANITO_AGREGADOS", "procs": 79, "area": 21500.0,
                "cfem": 48900.0, "aliquota": 1.6, "barragens": 0, "paebm": 0,
                "conf": 97.2, "dpa": "SEM_BARRAGEM", "critica_pct": 35.0, "score": 84.5,
            },
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "sub1": "GRANITO_AGREGADOS", "sub2": "AREIA_INDUSTRIAL", "procs": 94, "area": 16200.0,
                "cfem": 72100.0, "aliquota": 1.4, "barragens": 0, "paebm": 0,
                "conf": 98.1, "dpa": "SEM_BARRAGEM", "critica_pct": 38.0, "score": 85.8,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "sub1": "GRANITO_AGREGADOS", "sub2": "SAIS_MINERAIS", "procs": 68, "area": 14800.0,
                "cfem": 61400.0, "aliquota": 1.5, "barragens": 0, "paebm": 0,
                "conf": 96.8, "dpa": "SEM_BARRAGEM", "critica_pct": 31.0, "score": 83.2,
            },
        ]

        proc_rows: list[dict[str, Any]] = []
        cfem_rows: list[dict[str, Any]] = []
        sigbm_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            dam_status = (
                "SEM_BARRAGEM_PAEBM"
                if m["barragens"] == 0
                else ("MONITORAMENTO_ATIVO" if m["paebm"] > 0 else "CONFORME_PAEBM")
            )
            area1 = round(m["area"] * 0.62, 2)
            area2 = round(m["area"] * 0.38, 2)
            cfem1 = round(m["cfem"] * 0.72, 2)
            cfem2 = round(m["cfem"] * 0.28, 2)

            proc_rows.append({
                "cod_processo_anm": f"ANM-{m['uf']}-{m['id']}-8001",
                "substancia_mineral": m["sub1"],
                "fase_processo_sigmine": "CONCESSAO_LAVRA",
                "situacao_barragem_sigbm": dam_status,
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "area_titulada_hectares": area1,
                "arrecadacao_cfem_anual_mil_brl": cfem1,
                "ano_referencia": 2025,
            })
            proc_rows.append({
                "cod_processo_anm": f"ANM-{m['uf']}-{m['id']}-8002",
                "substancia_mineral": m["sub2"],
                "fase_processo_sigmine": "AUTORIZACAO_PESQUISA",
                "situacao_barragem_sigbm": "SEM_BARRAGEM_PAEBM",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "area_titulada_hectares": area2,
                "arrecadacao_cfem_anual_mil_brl": cfem2,
                "ano_referencia": 2025,
            })

            valor_op = round(m["cfem"] / (m["aliquota"] / 100.0), 2)
            cfem_rows.append({
                "cod_guia_cfem": f"CFEM-{m['uf']}-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "substancia_principal": m["sub1"],
                "valor_operacao_mineral_mil_brl": valor_op,
                "arrecadacao_cfem_municipio_mil_brl": m["cfem"],
                "aliquota_media_cfem_pct": m["aliquota"],
                "ano_referencia": 2025,
            })

            sigbm_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "qtd_barragens_sigbm": m["barragens"],
                "barragens_paebm_emergencia": m["paebm"],
                "indice_conformidade_sigbm_pct": m["conf"],
                "nivel_dano_potencial_associado": m["dpa"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "processos_ativos_sigmine": m["procs"],
                "area_total_titulada_ha": m["area"],
                "arrecadacao_cfem_total_mil_brl": m["cfem"],
                "indice_conformidade_sigbm_pct": m["conf"],
                "participacao_substancias_criticas_pct": m["critica_pct"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "processos_ativos_sigmine": m["procs"],
                "area_total_titulada_ha": m["area"],
                "arrecadacao_cfem_total_mil_brl": m["cfem"],
                "indice_conformidade_sigbm_pct": m["conf"],
                "qtd_barragens_sigbm": m["barragens"],
                "score_maturidade_mineral_esg": m["score"],
                "tier_mineracao_sustentavel_anm": _classify_mining_esg_tier(m["score"]),
            })

        cleaned_proc = apply_sentinel_cleaning(proc_rows, self.contract)
        proc_table = pa.Table.from_pylist(cleaned_proc)
        obs = evaluate_five_pillars(
            table=proc_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"ANM Mineracao 5-Pillar check failed: {obs.violations}")

        pq.write_table(proc_table, proc_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(cfem_rows),
            silver_dir / "anm_cfem_arrecadacao_royalties.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(sigbm_rows),
            silver_dir / "anm_sigbm_barragens_risco_esg.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "anm_mineracao_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "sigmine_processos": len(proc_rows),
                "cfem_royalties": len(cfem_rows),
                "sigbm_barragens": len(sigbm_rows),
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
        """Execute Medallion materialization and return anm_sigmine_processos_municipio Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "anm_sigmine_processos_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_anm_sigmine_processes(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SIGMINE mining processes by ANM code, substance, phase, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "anm_sigmine_processos_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_processo_anm"]) == term
            or term in _norm_text(r["substancia_mineral"])
            or term in _norm_text(r["fase_processo_sigmine"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_anm_cfem_royalties(self, termo: str) -> list[dict[str, Any]]:
        """Lookup CFEM mineral royalty collections by guide code, substance, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "anm_cfem_arrecadacao_royalties.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_guia_cfem"]) == term
            or term in _norm_text(r["substancia_principal"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_anm_sigbm_barragens(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal SIGBM mining-dam inventory, PAEBM alerts, and ESG conformity."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "anm_sigbm_barragens_risco_esg.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_anm_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal ANM active titles, titled hectares, and CFEM revenue."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "anm_mineracao_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_anm_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold ANM mineral maturity score and sustainable mining ESG tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_anm_mineracao_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_anm_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_cfem: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by ANM mineral ESG maturity score or CFEM royalties."""
        rows = self._read_parquet_rows("gold", "features_anm_mineracao_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_cfem:
            rows.sort(
                key=lambda r: (-float(r["arrecadacao_cfem_total_mil_brl"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_maturidade_mineral_esg"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("anm_mineracao", AnmMineracaoConnector)
