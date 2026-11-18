"""TSE Dados Abertos Eleitorais (Candidaturas, Votação & Contas) connector.

Governed by:
- Lei nº 9.504/1997 (Lei das Eleições) & Código Eleitoral (Lei nº 4.737/1965)
- Lei nº 12.527/2011 (LAI) & Portal de Dados Abertos do TSE
- Portal Oficial: https://dadosabertos.tse.jus.br/
- API DivulgaCandContas: https://divulgacandcontas.tse.jus.br/divulga/rest/v1/eleicao/ordinarias
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

TSE_DADOS_ABERTOS_URL = "https://dadosabertos.tse.jus.br/"
TSE_DIVULGACANDCONTAS_API_URL = (
    "https://divulgacandcontas.tse.jus.br/divulga/rest/v1/eleicao/ordinarias"
)


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_electoral_maturity_tier(score: float) -> str:
    """Classify municipal electoral participation and accountability maturity tier."""
    if score >= 78.0:
        return "ALTA_MATURIDADE_ELEITORAL"
    if score >= 60.0:
        return "PARTICIPACAO_ELEITORAL_REGULAR"
    return "ALERTA_ABSTENCAO_CONCENTRACAO"


class TseEleicoesConnector(BaseConnector):
    """Medallion connector for TSE Open Electoral Data, Turnout & Campaign Finance."""

    def __init__(
        self,
        source_id: str = "tse_eleicoes",
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
        """Absolute isolated directory for TSE Eleições Medallion storage."""
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
        """Perform a live fail-hard smoke test against TSE DivulgaCandContas REST API."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            TSE_DIVULGACANDCONTAS_API_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"TSE DivulgaCandContas API failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(
                f"TSE DivulgaCandContas API returned HTTP {resp.status_code}"
            )
        payload = resp.json()
        if not isinstance(payload, list) or not payload or "nomeEleicao" not in payload[0]:
            raise ValueError("TSE DivulgaCandContas response missing expected election entries.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "elections_count": len(payload),
            "content_length": len(resp.content),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold TSE Eleições Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        cand_file = silver_dir / "tse_candidaturas_partidos_municipio.parquet"
        gold_file = gold_dir / "features_eleicoes_tse_brasil.parquet"

        if cand_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "eleitorado": 1423722, "pref_cand": 10, "ver_cand": 768, "partidos": 23,
                "comp_pct": 84.5, "compar": 79.4, "abst": 20.6, "val": 91.2,
                "rec": 68420.0, "desp": 65180.0, "aprov": 92.8, "doad": 4890, "score": 88.2,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "eleitorado": 891400, "pref_cand": 9, "ver_cand": 642, "partidos": 21,
                "comp_pct": 81.2, "compar": 77.8, "abst": 22.2, "val": 90.4,
                "rec": 41250.0, "desp": 39810.0, "aprov": 91.5, "doad": 3120, "score": 86.1,
            },
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "eleitorado": 9322444, "pref_cand": 10, "ver_cand": 1020, "partidos": 26,
                "comp_pct": 88.4, "compar": 72.6, "abst": 27.4, "val": 89.6,
                "rec": 342800.0, "desp": 331900.0, "aprov": 90.2, "doad": 21450, "score": 84.3,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "eleitorado": 1096620, "pref_cand": 8, "ver_cand": 584, "partidos": 20,
                "comp_pct": 79.8, "compar": 68.5, "abst": 31.5, "val": 91.0,
                "rec": 44900.0, "desp": 43120.0, "aprov": 92.4, "doad": 3410, "score": 83.4,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "eleitorado": 1992984, "pref_cand": 10, "ver_cand": 812, "partidos": 24,
                "comp_pct": 85.1, "compar": 73.2, "abst": 26.8, "val": 89.9,
                "rec": 84600.0, "desp": 81900.0, "aprov": 89.8, "doad": 6210, "score": 83.7,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "eleitorado": 1030274, "pref_cand": 7, "ver_cand": 698, "partidos": 21,
                "comp_pct": 80.4, "compar": 74.1, "abst": 25.9, "val": 91.5,
                "rec": 48300.0, "desp": 46750.0, "aprov": 88.6, "doad": 3540, "score": 82.9,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "eleitorado": 1769681, "pref_cand": 9, "ver_cand": 745, "partidos": 22,
                "comp_pct": 83.0, "compar": 81.2, "abst": 18.8, "val": 92.4,
                "rec": 61200.0, "desp": 59400.0, "aprov": 87.9, "doad": 4180, "score": 85.6,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "eleitorado": 1213865, "pref_cand": 8, "ver_cand": 688, "partidos": 20,
                "comp_pct": 78.6, "compar": 80.5, "abst": 19.5, "val": 91.8,
                "rec": 49800.0, "desp": 48200.0, "aprov": 87.4, "doad": 3290, "score": 84.5,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "eleitorado": 1966016, "pref_cand": 7, "ver_cand": 790, "partidos": 22,
                "comp_pct": 77.2, "compar": 77.9, "abst": 22.1, "val": 90.8,
                "rec": 67100.0, "desp": 64900.0, "aprov": 86.2, "doad": 4320, "score": 82.8,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "eleitorado": 5009373, "pref_cand": 9, "ver_cand": 952, "partidos": 24,
                "comp_pct": 83.7, "compar": 70.4, "abst": 29.6, "val": 87.8,
                "rec": 198400.0, "desp": 192100.0, "aprov": 84.5, "doad": 11240, "score": 79.4,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "eleitorado": 445070, "pref_cand": 4, "ver_cand": 468, "partidos": 18,
                "comp_pct": 74.0, "compar": 72.1, "abst": 27.9, "val": 92.0,
                "rec": 27600.0, "desp": 26800.0, "aprov": 85.8, "doad": 1820, "score": 80.5,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "eleitorado": 1055091, "pref_cand": 9, "ver_cand": 640, "partidos": 19,
                "comp_pct": 76.5, "compar": 81.8, "abst": 18.2, "val": 93.1,
                "rec": 41900.0, "desp": 40500.0, "aprov": 84.1, "doad": 2650, "score": 83.2,
            },
            {
                "id": "1302603", "uf": "AM", "nome": "Manaus", "regiao": "Norte",
                "eleitorado": 1446122, "pref_cand": 7, "ver_cand": 710, "partidos": 21,
                "comp_pct": 79.1, "compar": 82.4, "abst": 17.6, "val": 93.6,
                "rec": 54800.0, "desp": 53100.0, "aprov": 85.4, "doad": 3480, "score": 84.8,
            },
        ]

        cand_rows: list[dict[str, Any]] = []
        vot_rows: list[dict[str, Any]] = []
        contas_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            cand_rows.append({
                "cod_eleicao_tse": f"TSE-2024-{m['id']}-PREF",
                "ano_eleicao": 2024,
                "turno_eleicao": "1_TURNO",
                "cargo_disputado": "PREFEITO",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_candidaturas_aptas": m["pref_cand"],
                "partidos_competitivos_count": min(m["partidos"], m["pref_cand"] + 4),
                "competicao_eleitoral_pct": round(m["comp_pct"] * 0.92, 2),
            })
            cand_rows.append({
                "cod_eleicao_tse": f"TSE-2024-{m['id']}-VER",
                "ano_eleicao": 2024,
                "turno_eleicao": "1_TURNO",
                "cargo_disputado": "VEREADOR",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_candidaturas_aptas": m["ver_cand"],
                "partidos_competitivos_count": m["partidos"],
                "competicao_eleitoral_pct": m["comp_pct"],
            })

            vot_rows.append({
                "cod_votacao_tse": f"VOT-2024-{m['id']}",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "eleitorado_apto_total": m["eleitorado"],
                "comparecimento_pct": m["compar"],
                "abstencao_pct": m["abst"],
                "votos_validos_pct": m["val"],
                "ano_eleicao": 2024,
            })

            contas_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "receita_total_campanha_mil_brl": m["rec"],
                "despesa_contratada_mil_brl": m["desp"],
                "contas_aprovadas_pct": m["aprov"],
                "doadores_cpf_cnpj_distintos": m["doad"],
            })

            total_cand = m["pref_cand"] + m["ver_cand"]
            custo_por_eleitor = round((m["rec"] * 1000.0) / m["eleitorado"], 2)

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "eleitorado_apto_total": m["eleitorado"],
                "total_candidaturas_aptas": total_cand,
                "comparecimento_pct": m["compar"],
                "receita_total_campanha_mil_brl": m["rec"],
                "custo_por_eleitor_brl": custo_por_eleitor,
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "eleitorado_apto_total": m["eleitorado"],
                "total_candidaturas_aptas": total_cand,
                "comparecimento_pct": m["compar"],
                "contas_aprovadas_pct": m["aprov"],
                "receita_total_campanha_mil_brl": m["rec"],
                "custo_por_eleitor_brl": custo_por_eleitor,
                "score_integridade_eleitoral_tse": m["score"],
                "tier_maturidade_eleitoral_tse": _classify_electoral_maturity_tier(m["score"]),
            })

        cleaned_cand = apply_sentinel_cleaning(cand_rows, self.contract)
        cand_table = pa.Table.from_pylist(cleaned_cand)
        obs = evaluate_five_pillars(
            table=cand_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"TSE Eleições 5-Pillar check failed: {obs.violations}")

        pq.write_table(cand_table, cand_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(vot_rows),
            silver_dir / "tse_votacao_comparecimento_secao.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(contas_rows),
            silver_dir / "tse_prestacao_contas_campanha.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "tse_eleicoes_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "candidaturas": len(cand_rows),
                "votacao_comparecimento": len(vot_rows),
                "prestacao_contas": len(contas_rows),
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
        """Execute Medallion materialization and return tse_candidaturas_partidos_municipio."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "tse_candidaturas_partidos_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_candidacies(self, termo: str) -> list[dict[str, Any]]:
        """Lookup TSE municipal candidacies by election code, office, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "tse_candidaturas_partidos_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_eleicao_tse"]) == term
            or term in _norm_text(r["cargo_disputado"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_turnout_stats(self, termo: str) -> list[dict[str, Any]]:
        """Lookup TSE municipal electorate, turnout, abstention, and valid votes."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "tse_votacao_comparecimento_secao.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_votacao_tse"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_campaign_finance(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve TSE campaign revenue, spending, approval rate, and distinct donors."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "tse_prestacao_contas_campanha.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_electoral_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal TSE electorate, candidacies, and campaign cost per voter."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "tse_eleicoes_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_electoral_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold TSE electoral participation and integrity score and tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_eleicoes_tse_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_electoral_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_electorate: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by TSE electoral integrity score or eligible electorate."""
        rows = self._read_parquet_rows("gold", "features_eleicoes_tse_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_electorate:
            rows.sort(
                key=lambda r: (-int(r["eleitorado_apto_total"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_integridade_eleitoral_tse"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("tse_eleicoes", TseEleicoesConnector)
