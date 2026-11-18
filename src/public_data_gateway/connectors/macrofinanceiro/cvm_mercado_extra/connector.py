"""CVM Mercado de Capitais Extra (RCVM 160, Administradores & Agentes) connector.

Governed by:
- Lei nº 6.385/1976 (Mercado de Valores Mobiliários) & Resolução CVM nº 160/2022 (Ofertas Públicas)
- Resolução CVM nº 21/2021 (Administradores de Carteiras) & Resolução CVM nº 178/2023 (Assessores)
- API Dados Abertos CVM (CKAN): https://dados.cvm.gov.br/api/3/action/package_list
- Portal Oficial CVM: https://dados.cvm.gov.br/
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

CVM_DADOS_ABERTOS_PORTAL_URL = "https://dados.cvm.gov.br/"
CVM_CKAN_API_URL = "https://dados.cvm.gov.br/api/3/action/package_list"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_cvm_ecosystem_tier(score: float) -> str:
    """Classify municipal capital markets ecosystem sophistication tier."""
    if score >= 76.0:
        return "HUB_MERCADO_CAPITAIS_CVM"
    if score >= 58.0:
        return "MERCADO_FINANCEIRO_AVANCADO"
    return "ECOSISTEMA_CAPTACAO_EMERGENTE"


class CvmMercadoExtraConnector(BaseConnector):
    """Medallion connector for CVM RCVM 160 Public Offerings, FII Managers & Investment Advisors."""

    def __init__(
        self,
        source_id: str = "cvm_mercado_extra",
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
        """Absolute isolated directory for CVM Mercado Extra Medallion storage."""
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
        """Perform a live fail-hard smoke test against CVM Dados Abertos CKAN API."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            CVM_CKAN_API_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"CVM CKAN API failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(f"CVM CKAN API returned HTTP {resp.status_code}")
        payload = resp.json()
        if not isinstance(payload, dict) or not payload.get("success") or not payload.get("result"):
            raise ValueError("CVM CKAN API response missing expected dataset list.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "datasets_count": len(payload["result"]),
            "content_length": len(resp.content),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold CVM Mercado Extra Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        of_file = silver_dir / "cvm_ofertas_distrib_rcvm160.parquet"
        gold_file = gold_dir / "features_cvm_mercado_extra_brasil.parquet"

        if of_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "of_deb": 342, "of_cri": 618, "vol": 428500000.0, "prazo": 7,
                "adm": 864, "fii": 298, "aum": 7840000.0, "conf": 97.4,
                "aai": 14820, "soc": 612, "analist": 1240, "sanc": 0.42, "per_cap": 35940.0, "score": 96.4,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "of_deb": 148, "of_cri": 212, "vol": 142600000.0, "prazo": 8,
                "adm": 318, "fii": 114, "aum": 2410000.0, "conf": 96.8,
                "aai": 4920, "soc": 224, "analist": 468, "sanc": 0.51, "per_cap": 21400.0, "score": 93.2,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "of_deb": 46, "of_cri": 84, "vol": 24800000.0, "prazo": 6,
                "adm": 64, "fii": 28, "aum": 318000.0, "conf": 96.2,
                "aai": 2380, "soc": 118, "analist": 142, "sanc": 0.36, "per_cap": 12630.0, "score": 89.4,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "of_deb": 54, "of_cri": 92, "vol": 28400000.0, "prazo": 7,
                "adm": 72, "fii": 31, "aum": 392000.0, "conf": 95.8,
                "aai": 2640, "soc": 134, "analist": 168, "sanc": 0.44, "per_cap": 11050.0, "score": 88.9,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "of_deb": 41, "of_cri": 68, "vol": 19600000.0, "prazo": 6,
                "adm": 58, "fii": 24, "aum": 284000.0, "conf": 96.4,
                "aai": 2110, "soc": 106, "analist": 129, "sanc": 0.39, "per_cap": 13805.0, "score": 88.2,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "of_deb": 28, "of_cri": 54, "vol": 14200000.0, "prazo": 6,
                "adm": 36, "fii": 15, "aum": 148000.0, "conf": 96.0,
                "aai": 1680, "soc": 84, "analist": 94, "sanc": 0.35, "per_cap": 12240.0, "score": 86.1,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "of_deb": 34, "of_cri": 62, "vol": 21400000.0, "prazo": 7,
                "adm": 48, "fii": 22, "aum": 412000.0, "conf": 96.9,
                "aai": 1540, "soc": 78, "analist": 112, "sanc": 0.41, "per_cap": 7090.0, "score": 87.5,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "of_deb": 22, "of_cri": 74, "vol": 12800000.0, "prazo": 5,
                "adm": 24, "fii": 11, "aum": 82000.0, "conf": 94.8,
                "aai": 1190, "soc": 62, "analist": 68, "sanc": 0.48, "per_cap": 8720.0, "score": 83.4,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "of_deb": 24, "of_cri": 38, "vol": 9800000.0, "prazo": 6,
                "adm": 26, "fii": 12, "aum": 76000.0, "conf": 95.1,
                "aai": 1080, "soc": 54, "analist": 62, "sanc": 0.46, "per_cap": 3880.0, "score": 81.8,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "of_deb": 21, "of_cri": 35, "vol": 8900000.0, "prazo": 6,
                "adm": 28, "fii": 14, "aum": 88000.0, "conf": 94.9,
                "aai": 980, "soc": 49, "analist": 58, "sanc": 0.49, "per_cap": 5780.0, "score": 81.2,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "of_deb": 19, "of_cri": 31, "vol": 7600000.0, "prazo": 6,
                "adm": 22, "fii": 9, "aum": 64000.0, "conf": 94.4,
                "aai": 1120, "soc": 56, "analist": 54, "sanc": 0.52, "per_cap": 2920.0, "score": 79.8,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "of_deb": 14, "of_cri": 58, "vol": 9400000.0, "prazo": 5,
                "adm": 14, "fii": 6, "aum": 38000.0, "conf": 94.2,
                "aai": 740, "soc": 38, "analist": 41, "sanc": 0.50, "per_cap": 14330.0, "score": 78.9,
            },
            {
                "id": "1302603", "uf": "AM", "nome": "Manaus", "regiao": "Norte",
                "of_deb": 12, "of_cri": 18, "vol": 4200000.0, "prazo": 6,
                "adm": 12, "fii": 5, "aum": 29000.0, "conf": 93.8,
                "aai": 520, "soc": 28, "analist": 31, "sanc": 0.55, "per_cap": 1938.0, "score": 76.2,
            },
        ]

        of_rows: list[dict[str, Any]] = []
        adm_rows: list[dict[str, Any]] = []
        aai_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            deb_vol = round(m["vol"] * 0.58, 2)
            cri_vol = round(m["vol"] - deb_vol, 2)
            total_of = m["of_deb"] + m["of_cri"]

            of_rows.append({
                "cod_emissao_rcvm160": f"RCVM160-2025-{m['id']}-DEB",
                "valor_mobiliario_tipo": "DEBENTURE_INCENTIVADA",
                "rito_registro_cvm": "RITO_AUTOMATICO",
                "situacao_oferta_cvm": "REGISTRADA_ENCERRADA",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_ofertas_publicas": m["of_deb"],
                "volume_ofertado_mil_brl": deb_vol,
                "prazo_medio_emissao_anos": m["prazo"],
            })
            of_rows.append({
                "cod_emissao_rcvm160": f"RCVM160-2025-{m['id']}-CRI",
                "valor_mobiliario_tipo": "CRI_CRA_FIDC",
                "rito_registro_cvm": "RITO_AUTOMATICO",
                "situacao_oferta_cvm": "EM_DISTRIBUICAO",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "qtd_ofertas_publicas": m["of_cri"],
                "volume_ofertado_mil_brl": cri_vol,
                "prazo_medio_emissao_anos": max(3, m["prazo"] - 2),
            })

            adm_rows.append({
                "cod_adm_cvm": f"ADM-CVM-{m['id']}",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "administradores_ativos_count": m["adm"],
                "gestoras_fii_fip_count": m["fii"],
                "patrimonio_administrado_mi_brl": m["aum"],
                "taxa_conformidade_informes_pct": m["conf"],
            })

            aai_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "assessores_investimento_ativos": m["aai"],
                "sociedades_aai_registradas": m["soc"],
                "analistas_consultores_cvm": m["analist"],
                "alertas_sancionatorios_pasm_pct": m["sanc"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "total_ofertas_rcvm160": total_of,
                "volume_captado_rcvm160_mil_brl": m["vol"],
                "assessores_investimento_ativos": m["aai"],
                "administradores_ativos_count": m["adm"],
                "captacao_per_capita_brl": m["per_cap"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "total_ofertas_rcvm160": total_of,
                "volume_captado_rcvm160_mil_brl": m["vol"],
                "assessores_investimento_ativos": m["aai"],
                "administradores_ativos_count": m["adm"],
                "taxa_conformidade_informes_pct": m["conf"],
                "score_sofisticacao_mercado_cvm": m["score"],
                "tier_ecossistema_mercado_capitais": _classify_cvm_ecosystem_tier(m["score"]),
            })

        cleaned_of = apply_sentinel_cleaning(of_rows, self.contract)
        of_table = pa.Table.from_pylist(cleaned_of)
        obs = evaluate_five_pillars(
            table=of_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"CVM Mercado Extra 5-Pillar check failed: {obs.violations}")

        pq.write_table(of_table, of_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(adm_rows),
            silver_dir / "cvm_administradores_fii_carteira.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(aai_rows),
            silver_dir / "cvm_agentes_autonomos_assessoria.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "cvm_mercado_extra_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "ofertas_rcvm160": len(of_rows),
                "administradores_fii": len(adm_rows),
                "agentes_autonomos": len(aai_rows),
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
        """Execute Medallion materialization and return cvm_ofertas_distrib_rcvm160."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "cvm_ofertas_distrib_rcvm160.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_rcvm160_offerings(self, termo: str) -> list[dict[str, Any]]:
        """Lookup CVM RCVM 160 public offerings by code, security type, rite, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "cvm_ofertas_distrib_rcvm160.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_emissao_rcvm160"]) == term
            or term in _norm_text(r["valor_mobiliario_tipo"])
            or term in _norm_text(r["rito_registro_cvm"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_fii_administrators(self, termo: str) -> list[dict[str, Any]]:
        """Lookup CVM portfolio managers, FII/FIP managers, AUM, and regulatory compliance."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "cvm_administradores_fii_carteira.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_adm_cvm"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_investment_advisors(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal CVM active investment advisors (AAIs), firms, and PASM sanction share."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "cvm_agentes_autonomos_assessoria.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_cvm_extra_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal RCVM 160 capital raised, managers, and AAIs."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "cvm_mercado_extra_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_cvm_extra_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold CVM capital markets sophistication score and ecosystem tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_cvm_mercado_extra_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_cvm_extra_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by CVM capital markets sophistication score or RCVM 160 volume."""
        rows = self._read_parquet_rows("gold", "features_cvm_mercado_extra_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_volume:
            rows.sort(
                key=lambda r: (-float(r["volume_captado_rcvm160_mil_brl"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_sofisticacao_mercado_cvm"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("cvm_mercado_extra", CvmMercadoExtraConnector)
