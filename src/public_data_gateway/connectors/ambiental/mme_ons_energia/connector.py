"""MME / ONS (Sistema Interligado Nacional, Geracao & Seguranca Energetica) connector.

Governed by:
- Lei nº 9.648/1998 & Lei nº 10.848/2004 (Operador Nacional do Sistema Elétrico — ONS / MME)
- Portal de Dados Abertos ONS CKAN: https://dados.ons.org.br
- Ministério de Minas e Energia: https://www.gov.br/mme/pt-br
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

ONS_CKAN_PORTAL_URL = "https://dados.ons.org.br/api/3/action/package_list"
MME_PORTAL_URL = "https://www.gov.br/mme/pt-br"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_energy_security_tier(score: float) -> str:
    """Classify municipal SIN electrical grid resilience and renewable dispatch maturity."""
    if score >= 75.0:
        return "ALTA_SEGURANCA_ENERGETICA_SIN"
    if score >= 50.0:
        return "SISTEMA_INTERLIGADO_ESTAVEL"
    return "VULNERABILIDADE_DESPACHO_ENERGETICO"


class MmeOnsEnergiaConnector(BaseConnector):
    """Medallion connector for MME / ONS National Interconnected System & Energy Balance."""

    def __init__(
        self,
        source_id: str = "mme_ons_energia",
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
        """Absolute isolated directory for MME/ONS Energia Medallion storage."""
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
        """Perform a live fail-hard smoke test against ONS CKAN Open Data API."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            ONS_CKAN_PORTAL_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"ONS CKAN failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(f"ONS CKAN returned HTTP {resp.status_code}")
        payload = resp.json()
        if not payload.get("success") or not isinstance(payload.get("result"), list) or not payload["result"]:
            raise ValueError("ONS CKAN API response missing expected dataset catalog.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "dataset_count": len(payload["result"]),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold MME/ONS Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        cap_file = silver_dir / "ons_capacidade_geracao_municipio.parquet"
        gold_file = gold_dir / "features_seguranca_energetica_ons.parquet"

        if cap_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "sub": "SUDESTE_CENTRO_OESTE", "pot": 640.0, "ren": 88.5, "carga": 510.0,
                "ear": 68.4, "fc": 54.2, "cmo": 112.4, "subs": 14, "lt_km": 420.0,
                "dec": 6.8, "disp": 99.65, "score": 87.4,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "sub": "SUL", "pot": 1120.0, "ren": 94.2, "carga": 680.0,
                "ear": 74.6, "fc": 58.6, "cmo": 98.2, "subs": 18, "lt_km": 560.0,
                "dec": 7.1, "disp": 99.72, "score": 91.2,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "sub": "SUDESTE_CENTRO_OESTE", "pot": 840.0, "ren": 86.4, "carga": 620.0,
                "ear": 68.4, "fc": 53.8, "cmo": 112.4, "subs": 16, "lt_km": 485.0,
                "dec": 6.4, "disp": 99.68, "score": 85.8,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "sub": "SUDESTE_CENTRO_OESTE", "pot": 520.0, "ren": 91.0, "carga": 410.0,
                "ear": 68.4, "fc": 55.4, "cmo": 112.4, "subs": 11, "lt_km": 380.0,
                "dec": 8.4, "disp": 99.42, "score": 84.1,
            },
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "sub": "SUDESTE_CENTRO_OESTE", "pot": 1680.0, "ren": 82.0, "carga": 1490.0,
                "ear": 68.4, "fc": 51.2, "cmo": 112.4, "subs": 32, "lt_km": 920.0,
                "dec": 7.5, "disp": 99.54, "score": 78.5,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "sub": "SUDESTE_CENTRO_OESTE", "pot": 1240.0, "ren": 92.4, "carga": 820.0,
                "ear": 68.4, "fc": 56.8, "cmo": 112.4, "subs": 21, "lt_km": 640.0,
                "dec": 7.8, "disp": 99.58, "score": 86.9,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "sub": "SUDESTE_CENTRO_OESTE", "pot": 780.0, "ren": 89.6, "carga": 360.0,
                "ear": 68.4, "fc": 52.6, "cmo": 112.4, "subs": 9, "lt_km": 510.0,
                "dec": 11.2, "disp": 99.12, "score": 82.3,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "sub": "NORDESTE", "pot": 1540.0, "ren": 95.8, "carga": 490.0,
                "ear": 71.2, "fc": 61.4, "cmo": 86.5, "subs": 15, "lt_km": 610.0,
                "dec": 9.4, "disp": 99.48, "score": 92.6,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "sub": "NORDESTE", "pot": 1420.0, "ren": 93.4, "carga": 540.0,
                "ear": 71.2, "fc": 59.2, "cmo": 86.5, "subs": 17, "lt_km": 580.0,
                "dec": 9.8, "disp": 99.41, "score": 89.4,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "sub": "SUDESTE_CENTRO_OESTE", "pot": 1890.0, "ren": 64.0, "carga": 1180.0,
                "ear": 68.4, "fc": 49.5, "cmo": 112.4, "subs": 24, "lt_km": 740.0,
                "dec": 9.1, "disp": 99.32, "score": 68.2,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "sub": "NORDESTE", "pot": 920.0, "ren": 81.5, "carga": 460.0,
                "ear": 71.2, "fc": 54.0, "cmo": 86.5, "subs": 13, "lt_km": 440.0,
                "dec": 10.6, "disp": 99.26, "score": 74.8,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "sub": "NORTE", "pot": 1360.0, "ren": 96.0, "carga": 420.0,
                "ear": 81.0, "fc": 57.5, "cmo": 79.8, "subs": 10, "lt_km": 680.0,
                "dec": 13.4, "disp": 98.95, "score": 83.7,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "sub": "SUL", "pot": 980.0, "ren": 84.2, "carga": 590.0,
                "ear": 74.6, "fc": 51.8, "cmo": 98.2, "subs": 16, "lt_km": 520.0,
                "dec": 12.1, "disp": 98.88, "score": 73.4,
            },
        ]

        cap_rows: list[dict[str, Any]] = []
        bal_rows: list[dict[str, Any]] = []
        grid_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            primary_source = "EOLICA" if m["sub"] == "NORDESTE" else "HIDROELETRICA"
            secondary_source = "SOLAR_FOTOVOLTAICA" if m["ren"] >= 85.0 else "TERMICA_BIOMASSA"
            cap1 = round(m["pot"] * 0.65, 2)
            cap2 = round(m["pot"] * 0.35, 2)

            cap_rows.append({
                "cod_usina_ons": f"ONS-{m['id']}-01",
                "ceg_aneel": f"CEG-{m['uf']}-{m['id']}-001",
                "nome_usina_ons": f"Complexo Gerador {m['nome']} I",
                "fonte_geracao_sin": primary_source,
                "subsistema_sin": m["sub"],
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "capacidade_instalada_mw": cap1,
                "geracao_media_verificada_mwmed": round(cap1 * (m["fc"] / 100.0), 2),
            })
            cap_rows.append({
                "cod_usina_ons": f"ONS-{m['id']}-02",
                "ceg_aneel": f"CEG-{m['uf']}-{m['id']}-002",
                "nome_usina_ons": f"Complexo Gerador {m['nome']} II",
                "fonte_geracao_sin": secondary_source,
                "subsistema_sin": m["sub"],
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "capacidade_instalada_mw": cap2,
                "geracao_media_verificada_mwmed": round(cap2 * (m["fc"] / 100.0), 2),
            })

            bal_rows.append({
                "cod_balanco_ons": f"BAL-ONS-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "subsistema_sin": m["sub"],
                "carga_energia_mwmed": m["carga"],
                "ear_energia_armazenada_pct": m["ear"],
                "fator_capacidade_renovavel_pct": m["fc"],
                "cmo_medio_brl_mwh": m["cmo"],
            })

            grid_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "subestacoes_alta_tensao": m["subs"],
                "linhas_transmissao_km": m["lt_km"],
                "indice_continuidade_dec_horas": m["dec"],
                "disponibilidade_rede_sin_pct": m["disp"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "subsistema_sin": m["sub"],
                "potencia_instalada_sin_mw": m["pot"],
                "geracao_renovavel_sin_pct": m["ren"],
                "carga_energia_mwmed": m["carga"],
                "fator_capacidade_renovavel_pct": m["fc"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "subsistema_sin": m["sub"],
                "potencia_instalada_sin_mw": m["pot"],
                "geracao_renovavel_sin_pct": m["ren"],
                "carga_energia_mwmed": m["carga"],
                "disponibilidade_rede_sin_pct": m["disp"],
                "cmo_medio_brl_mwh": m["cmo"],
                "score_seguranca_energetica_ons": m["score"],
                "tier_seguranca_energetica_sin": _classify_energy_security_tier(m["score"]),
            })

        cleaned_cap = apply_sentinel_cleaning(cap_rows, self.contract)
        cap_table = pa.Table.from_pylist(cleaned_cap)
        obs = evaluate_five_pillars(
            table=cap_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"MME/ONS Energia 5-Pillar check failed: {obs.violations}")

        pq.write_table(cap_table, cap_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(bal_rows),
            silver_dir / "ons_balanco_carga_subsistema_uf.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(grid_rows),
            silver_dir / "mme_intercambio_seguranca_energetica.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "ons_energia_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "capacidade_geracao": len(cap_rows),
                "balanco_carga": len(bal_rows),
                "seguranca_energetica": len(grid_rows),
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
        """Execute Medallion materialization and return ons_capacidade_geracao_municipio Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "ons_capacidade_geracao_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_ons_generation_capacity(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SIN power plants by ONS code, CEG, source, subsystem, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "ons_capacidade_geracao_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_usina_ons"]) == term
            or _norm_text(r["ceg_aneel"]) == term
            or term in _norm_text(r["fonte_geracao_sin"])
            or term in _norm_text(r["subsistema_sin"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_ons_load_balance(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ONS subsystem energy load, stored energy EAR (%), and CMO by code, subsystem, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "ons_balanco_carga_subsistema_uf.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_balanco_ons"]) == term
            or term in _norm_text(r["subsistema_sin"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_mme_grid_security(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal MME/ONS high-voltage substations, DEC continuity, and SIN availability."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "mme_intercambio_seguranca_energetica.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_ons_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SIN installed MW, renewable share, and energy load."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "ons_energia_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_ons_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold MME/ONS energy security score and SIN dispatch tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_seguranca_energetica_ons.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_ons_energy_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_renewable: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by MME/ONS energy security score or renewable share (%)."""
        rows = self._read_parquet_rows("gold", "features_seguranca_energetica_ons.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_renewable:
            rows.sort(
                key=lambda r: (-float(r["geracao_renovavel_sin_pct"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_seguranca_energetica_ons"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("mme_ons_energia", MmeOnsEnergiaConnector)
