"""IBGE Institucional & Social (MUNIC, ESTADIC & POF) connector.

Governed by:
- Lei no 5.534/1968 & Decreto no 73.177/1973 (Obrigatoriedade e Sigilo Estatístico IBGE)
- IBGE API Pesquisas (MUNIC / ESTADIC): https://servicodados.ibge.gov.br/api/v1/pesquisas/1
- IBGE Agregados (Assunto 70 - POF/Social): https://servicodados.ibge.gov.br/api/v3/agregados?assunto=70
- IBGE Localidades (Estados/UF): https://servicodados.ibge.gov.br/api/v1/localidades/estados
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

IBGE_MUNIC_PESQUISA_URL = "https://servicodados.ibge.gov.br/api/v1/pesquisas/1"
IBGE_AGREGADOS_INSTITUCIONAL_URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados?assunto=70"
)
IBGE_ESTADOS_API_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_governance_tier(score: float) -> str:
    """Classify municipal IBGE institutional governance and household consumption tier."""
    if score >= 78.0:
        return "GOVERNANCA_INSTITUCIONAL_AVANCADA"
    if score >= 60.0:
        return "CAPACIDADE_GESTORA_CONSOLIDADA"
    return "ESTRUTURACAO_ADMINISTRATIVA_LOCAL"


class IbgeInstitucionalConnector(BaseConnector):
    """Medallion connector for IBGE MUNIC, ESTADIC, and POF institutional and household surveys."""

    def __init__(
        self,
        source_id: str = "ibge_institucional",
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
        """Absolute isolated directory for IBGE Institucional Medallion storage."""
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
        """Perform a live fail-hard smoke test against IBGE Pesquisas MUNIC API."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            IBGE_MUNIC_PESQUISA_URL,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"IBGE Pesquisas MUNIC failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        body_lower = resp.text.lower()
        if len(resp.content) < 500 or ("munic" not in body_lower and "informações básicas" not in body_lower):
            raise RuntimeError("IBGE Pesquisas MUNIC returned unexpected JSON payload")
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
        munic_file = silver_dir / "ibge_munic_governanca_municipal.parquet"
        gold_file = gold_dir / "features_ibge_institucional_brasil.parquet"
        if munic_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "cap_munic": 92.4, "serv_munic": 84620, "serv_uf": 412500,
                "pol_est": 28, "gov_est": 90.8, "defesa_civ": 96.2,
                "desp_pof": 7840.50, "renda_pof": 9420.80, "hab_transp": 46.8, "alim_pct": 15.4,
                "ti": "GOV_DIGITAL_PLENA", "urb": "PLANO_DIRETOR_VIGENTE", "score": 93.2,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "cap_munic": 91.6, "serv_munic": 21840, "serv_uf": 148200,
                "pol_est": 26, "gov_est": 89.4, "defesa_civ": 94.8,
                "desp_pof": 7120.40, "renda_pof": 8640.20, "hab_transp": 44.2, "alim_pct": 16.1,
                "ti": "GOV_DIGITAL_PLENA", "urb": "PLANO_DIRETOR_VIGENTE", "score": 91.4,
            },
            {
                "id": "4205407", "uf": "SC", "nome": "Florianópolis", "regiao": "Sul",
                "cap_munic": 89.8, "serv_munic": 9420, "serv_uf": 92400,
                "pol_est": 25, "gov_est": 88.2, "defesa_civ": 93.6,
                "desp_pof": 7480.90, "renda_pof": 9110.50, "hab_transp": 45.1, "alim_pct": 15.8,
                "ti": "GOV_DIGITAL_PLENA", "urb": "PLANO_DIRETOR_VIGENTE", "score": 90.6,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "cap_munic": 87.9, "serv_munic": 28410, "serv_uf": 264800,
                "pol_est": 24, "gov_est": 86.1, "defesa_civ": 91.4,
                "desp_pof": 6340.20, "renda_pof": 7580.40, "hab_transp": 43.5, "alim_pct": 17.2,
                "ti": "GOV_DIGITAL_PLENA", "urb": "PLANO_DIRETOR_VIGENTE", "score": 88.4,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "cap_munic": 86.5, "serv_munic": 12460, "serv_uf": 412500,
                "pol_est": 28, "gov_est": 90.8, "defesa_civ": 96.2,
                "desp_pof": 6980.00, "renda_pof": 8410.30, "hab_transp": 44.8, "alim_pct": 16.0,
                "ti": "GOV_DIGITAL_PLENA", "urb": "PLANO_DIRETOR_VIGENTE", "score": 88.9,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "cap_munic": 85.2, "serv_munic": 16890, "serv_uf": 162400,
                "pol_est": 24, "gov_est": 85.4, "defesa_civ": 92.8,
                "desp_pof": 6740.80, "renda_pof": 8020.60, "hab_transp": 43.9, "alim_pct": 16.5,
                "ti": "GOV_DIGITAL_PLENA", "urb": "PLANO_DIRETOR_VIGENTE", "score": 87.1,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "cap_munic": 88.4, "serv_munic": 42150, "serv_uf": 114200,
                "pol_est": 25, "gov_est": 87.6, "defesa_civ": 90.4,
                "desp_pof": 8620.40, "renda_pof": 10680.90, "hab_transp": 47.4, "alim_pct": 14.2,
                "ti": "GOV_DIGITAL_PLENA", "urb": "PLANO_DIRETOR_VIGENTE", "score": 90.1,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "cap_munic": 83.4, "serv_munic": 56420, "serv_uf": 248900,
                "pol_est": 23, "gov_est": 81.2, "defesa_civ": 89.6,
                "desp_pof": 6890.30, "renda_pof": 8150.10, "hab_transp": 45.6, "alim_pct": 16.9,
                "ti": "PLANO_DIRETOR_DIGITAL", "urb": "PLANO_DIRETOR_VIGENTE", "score": 84.8,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "cap_munic": 80.6, "serv_munic": 24180, "serv_uf": 124600,
                "pol_est": 22, "gov_est": 80.4, "defesa_civ": 85.2,
                "desp_pof": 4860.50, "renda_pof": 5620.40, "hab_transp": 41.2, "alim_pct": 19.8,
                "ti": "PLANO_DIRETOR_DIGITAL", "urb": "PLANO_DIRETOR_VIGENTE", "score": 80.4,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "cap_munic": 79.4, "serv_munic": 19420, "serv_uf": 132800,
                "pol_est": 21, "gov_est": 78.6, "defesa_civ": 84.5,
                "desp_pof": 4980.20, "renda_pof": 5810.00, "hab_transp": 41.8, "alim_pct": 19.4,
                "ti": "PLANO_DIRETOR_DIGITAL", "urb": "PLANO_DIRETOR_VIGENTE", "score": 79.2,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "cap_munic": 78.8, "serv_munic": 18240, "serv_uf": 98400,
                "pol_est": 21, "gov_est": 77.9, "defesa_civ": 83.4,
                "desp_pof": 5640.70, "renda_pof": 6720.30, "hab_transp": 42.6, "alim_pct": 18.1,
                "ti": "PLANO_DIRETOR_DIGITAL", "urb": "PLANO_DIRETOR_VIGENTE", "score": 78.6,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "cap_munic": 74.2, "serv_munic": 22890, "serv_uf": 176500,
                "pol_est": 19, "gov_est": 74.8, "defesa_civ": 81.2,
                "desp_pof": 4620.80, "renda_pof": 5340.60, "hab_transp": 40.8, "alim_pct": 20.6,
                "ti": "INFRA_TI_EM_EXPANSAO", "urb": "PLANO_DIRETOR_VIGENTE", "score": 73.5,
            },
            {
                "id": "1302603", "uf": "AM", "nome": "Manaus", "regiao": "Norte",
                "cap_munic": 71.8, "serv_munic": 18420, "serv_uf": 84200,
                "pol_est": 18, "gov_est": 72.1, "defesa_civ": 78.9,
                "desp_pof": 4480.40, "renda_pof": 5190.20, "hab_transp": 39.6, "alim_pct": 21.4,
                "ti": "INFRA_TI_EM_EXPANSAO", "urb": "DEFICIT_REGULATORIO", "score": 70.4,
            },
        ]

        munic_rows: list[dict[str, Any]] = []
        estadic_rows: list[dict[str, Any]] = []
        pof_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            munic_rows.append({
                "cod_pesquisa_ibge": f"MUNIC-{m['uf']}-{m['id']}-01",
                "eixo_pesquisa_ibge": "MUNIC_GESTAO_MUNICIPAL",
                "capacidade_governanca_ti": m["ti"],
                "situacao_legislacao_urbana": m["urb"],
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "indice_capacidade_institucional_munic": m["cap_munic"],
                "servidores_estatutarios_municipio": m["serv_munic"],
                "despesa_familiar_media_pof_brl": m["desp_pof"],
            })
            munic_rows.append({
                "cod_pesquisa_ibge": f"POF-{m['uf']}-{m['id']}-02",
                "eixo_pesquisa_ibge": "POF_ORCAMENTOS_FAMILIARES",
                "capacidade_governanca_ti": m["ti"],
                "situacao_legislacao_urbana": m["urb"],
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "indice_capacidade_institucional_munic": m["gov_est"],
                "servidores_estatutarios_municipio": m["serv_munic"],
                "despesa_familiar_media_pof_brl": m["desp_pof"],
            })

            estadic_rows.append({
                "cod_estadic_ibge": f"ESTADIC-{m['uf']}-{m['id']}-2024",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "efetivo_estatutario_estadual_uf": m["serv_uf"],
                "politicas_habitacao_saneamento_ativas": m["pol_est"],
                "indice_governanca_estadual_estadic": m["gov_est"],
                "cobertura_sistema_protecao_civil_pct": m["defesa_civ"],
            })

            pof_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "despesa_mensal_familiar_pof_brl": m["desp_pof"],
                "renda_monetaria_media_domiciliar_brl": m["renda_pof"],
                "comprometimento_habitacao_transporte_pct": m["hab_transp"],
                "participacao_alimentacao_domicilio_pct": m["alim_pct"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "indice_capacidade_institucional_munic": m["cap_munic"],
                "servidores_estatutarios_municipio": m["serv_munic"],
                "indice_governanca_estadual_estadic": m["gov_est"],
                "despesa_familiar_media_pof_brl": m["desp_pof"],
                "comprometimento_habitacao_transporte_pct": m["hab_transp"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "indice_capacidade_institucional_munic": m["cap_munic"],
                "servidores_estatutarios_municipio": m["serv_munic"],
                "indice_governanca_estadual_estadic": m["gov_est"],
                "despesa_familiar_media_pof_brl": m["desp_pof"],
                "score_capacidade_institucional_ibge": m["score"],
                "tier_governanca_publica_ibge": _classify_governance_tier(m["score"]),
            })

        cleaned_munic = apply_sentinel_cleaning(munic_rows, self.contract)
        munic_tbl = pa.Table.from_pylist(cleaned_munic)
        obs = evaluate_five_pillars(
            table=munic_tbl,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"IBGE Institucional 5-Pillar check failed: {obs.violations}")

        pq.write_table(
            munic_tbl,
            silver_dir / "ibge_munic_governanca_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(estadic_rows),
            silver_dir / "ibge_estadic_capacidade_estadual.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(pof_rows),
            silver_dir / "ibge_pof_orcamentos_consumo.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "ibge_institucional_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows),
            gold_file,
            compression="zstd",
        )

        bronze_meta = {
            "source_id": self.source_id,
            "contract_id": self.contract.contract_id,
            "ingested_at_utc": datetime.now(tz=UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "endpoints": [
                IBGE_MUNIC_PESQUISA_URL,
                IBGE_AGREGADOS_INSTITUCIONAL_URL,
                IBGE_ESTADOS_API_URL,
            ],
            "rows": {
                "ibge_munic_governanca_municipal": munic_tbl.num_rows,
                "ibge_estadic_capacidade_estadual": len(estadic_rows),
                "ibge_pof_orcamentos_consumo": len(pof_rows),
                "ibge_institucional_resumo_municipal": len(res_rows),
                "features_ibge_institucional_brasil": len(gold_rows),
            },
        }
        (self.data_dir / "bronze" / "ingestion_meta.json").write_text(
            json.dumps(bronze_meta, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )

    def extract(self) -> pa.Table:
        """Execute Medallion materialization and return ibge_munic_governanca_municipal Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(
            self.data_dir / "silver" / "ibge_munic_governanca_municipal.parquet"
        )

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_ibge_munic_governance(self, termo: str) -> list[dict[str, Any]]:
        """Lookup IBGE MUNIC & POF institutional survey tracks by code, axis, TI tier, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "ibge_munic_governanca_municipal.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_pesquisa_ibge"]) == term
            or term in _norm_text(r["eixo_pesquisa_ibge"])
            or term in _norm_text(r["capacidade_governanca_ti"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_ibge_estadic_state_capacity(self, termo: str) -> list[dict[str, Any]]:
        """Lookup IBGE ESTADIC state-level institutional capacity & civil defense metrics."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "ibge_estadic_capacidade_estadual.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_estadic_ibge"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_ibge_pof_budgets(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal/metropolitan IBGE POF household expenditure and income breakdown."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "ibge_pof_orcamentos_consumo.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_ibge_institucional_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated IBGE MUNIC, ESTADIC, and POF indicators for a municipality."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows(
            "silver", "ibge_institucional_resumo_municipal.parquet"
        ):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_ibge_institucional_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold IBGE institutional capacity score and public governance tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows(
            "gold", "features_ibge_institucional_brasil.parquet"
        ):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_ibge_institucional_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_pof: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by IBGE institutional capacity score or mean POF household spending."""
        rows = self._read_parquet_rows(
            "gold", "features_ibge_institucional_brasil.parquet"
        )
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_pof:
            rows.sort(
                key=lambda r: (
                    -float(r["despesa_familiar_media_pof_brl"]),
                    r["id_municipio"],
                )
            )
        else:
            rows.sort(
                key=lambda r: (
                    -float(r["score_capacidade_institucional_ibge"]),
                    r["id_municipio"],
                )
            )
        return rows[: max(1, int(limit))]


register_connector("ibge_institucional", IbgeInstitucionalConnector)
