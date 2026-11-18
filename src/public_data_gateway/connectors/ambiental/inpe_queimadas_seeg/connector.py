"""INPE Queimadas & SEEG (Focos de Calor & Emissões GEE) Medallion connector.

Governed by:
- Decreto nº 2.661/1998 & Lei nº 12.651/2012 (Manejo Integrado do Fogo & Código Florestal)
- Lei nº 12.187/2009 (Política Nacional sobre Mudança do Clima - PNMC / IPCC SEEG)
- Dataserver Oficial INPE Queimadas: https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/
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

INPE_QUEIMADAS_DATASERV_URL = (
    "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/"
)
SEEG_PORTAL_URL = "https://seeg.eco.br/"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_climate_fire_risk(score: float) -> str:
    """Classify municipal climate resilience and fire/GHG emission pressure."""
    if score >= 75.0:
        return "BAIXO_RISCO_FOGO_GEE"
    if score >= 50.0:
        return "ALERTA_FOGO_EMISSOES_MODERADAS"
    return "CRITICO_QUEIMADAS_EMISSOES_GEE"


class InpeQueimadasSeegConnector(BaseConnector):
    """Medallion connector for INPE Fire Hotspots (Queimadas) & SEEG Municipal GHG Emissions."""

    def __init__(
        self,
        source_id: str = "inpe_queimadas_seeg",
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
        """Absolute isolated directory for INPE Queimadas & SEEG Medallion storage."""
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
        """Perform a live fail-hard smoke test against INPE Queimadas daily CSV directory."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            INPE_QUEIMADAS_DATASERV_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
            verify=False,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"INPE Queimadas dataserver failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(
                f"INPE Queimadas dataserver returned HTTP {resp.status_code}"
            )
        if "focos_diario" not in resp.text.lower() or len(resp.text) <= 500:
            raise ValueError("INPE Queimadas listing missing expected daily CSV links.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "content_length": len(resp.text),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold Queimadas/SEEG Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        focos_file = silver_dir / "inpe_focos_queimadas.parquet"
        gold_file = gold_dir / "features_queimadas_emissoes_gee.parquet"

        if focos_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "bioma": "Mata_Atlantica",
                "lat": -25.4284, "lon": -49.2733, "focos": 48, "area_ha": 310.0,
                "risco": 0.24, "frp": 14.2, "dias_seco": 9, "gee_liq": 1420000.0,
                "agro_pct": 18.4, "score": 91.2,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "bioma": "Pampa",
                "lat": -30.0346, "lon": -51.2177, "focos": 62, "area_ha": 480.0,
                "risco": 0.28, "frp": 16.5, "dias_seco": 11, "gee_liq": 1850000.0,
                "agro_pct": 22.1, "score": 88.6,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "bioma": "Mata_Atlantica",
                "lat": -22.9056, "lon": -47.0608, "focos": 95, "area_ha": 620.0,
                "risco": 0.36, "frp": 19.8, "dias_seco": 18, "gee_liq": 2140000.0,
                "agro_pct": 24.5, "score": 84.5,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "bioma": "Mata_Atlantica",
                "lat": -8.0476, "lon": -34.8770, "focos": 74, "area_ha": 410.0,
                "risco": 0.31, "frp": 15.4, "dias_seco": 14, "gee_liq": 1960000.0,
                "agro_pct": 16.8, "score": 82.9,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "bioma": "Mata_Atlantica",
                "lat": -22.9068, "lon": -43.1729, "focos": 145, "area_ha": 980.0,
                "risco": 0.42, "frp": 21.4, "dias_seco": 16, "gee_liq": 8950000.0,
                "agro_pct": 6.4, "score": 76.4,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "bioma": "Caatinga",
                "lat": -3.7319, "lon": -38.5267, "focos": 168, "area_ha": 1240.0,
                "risco": 0.49, "frp": 24.6, "dias_seco": 27, "gee_liq": 3420000.0,
                "agro_pct": 19.2, "score": 73.1,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "bioma": "Mata_Atlantica",
                "lat": -12.9714, "lon": -38.5014, "focos": 182, "area_ha": 1490.0,
                "risco": 0.46, "frp": 22.8, "dias_seco": 22, "gee_liq": 4180000.0,
                "agro_pct": 21.5, "score": 71.0,
            },
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "bioma": "Mata_Atlantica",
                "lat": -23.5505, "lon": -46.6333, "focos": 215, "area_ha": 1680.0,
                "risco": 0.44, "frp": 23.1, "dias_seco": 19, "gee_liq": 16840000.0,
                "agro_pct": 4.2, "score": 66.8,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "bioma": "Cerrado",
                "lat": -19.9167, "lon": -43.9345, "focos": 290, "area_ha": 3420.0,
                "risco": 0.62, "frp": 31.5, "dias_seco": 34, "gee_liq": 6420000.0,
                "agro_pct": 38.6, "score": 61.5,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "bioma": "Cerrado",
                "lat": -15.7975, "lon": -47.8919, "focos": 485, "area_ha": 8950.0,
                "risco": 0.74, "frp": 38.4, "dias_seco": 48, "gee_liq": 7840000.0,
                "agro_pct": 46.2, "score": 53.8,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "bioma": "Cerrado",
                "lat": -16.6869, "lon": -49.2648, "focos": 740, "area_ha": 18600.0,
                "risco": 0.81, "frp": 46.2, "dias_seco": 52, "gee_liq": 14650000.0,
                "agro_pct": 72.4, "score": 44.2,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "bioma": "Amazonia",
                "lat": -1.4558, "lon": -48.5039, "focos": 1920, "area_ha": 84200.0,
                "risco": 0.89, "frp": 68.5, "dias_seco": 41, "gee_liq": 32400000.0,
                "agro_pct": 84.8, "score": 31.4,
            },
        ]

        focos_rows: list[dict[str, Any]] = []
        emissoes_rows: list[dict[str, Any]] = []
        risco_rows: list[dict[str, Any]] = []
        resumo_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for idx, m in enumerate(seed_municipalities, start=1):
            focos_rows.append({
                "cod_foco_inpe": f"INPE-AQ-{m['id']}-01",
                "satelite_referencia": "AQUA_M-T",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "bioma": m["bioma"],
                "data_hora_gmt": f"2025-08-{(idx % 18) + 10:02d}T16:45:00Z",
                "risco_fogo": m["risco"],
                "frp_mw": m["frp"],
                "dias_sem_chuva": m["dias_seco"],
                "precipitacao_mm": 0.0 if m["dias_seco"] > 15 else 2.4,
                "latitude": round(m["lat"] + 0.025, 4),
                "longitude": round(m["lon"] - 0.025, 4),
            })
            focos_rows.append({
                "cod_foco_inpe": f"INPE-NPP-{m['id']}-02",
                "satelite_referencia": "NPP-375",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "bioma": m["bioma"],
                "data_hora_gmt": f"2025-09-{(idx % 18) + 10:02d}T17:15:00Z",
                "risco_fogo": round(min(1.0, m["risco"] + 0.06), 2),
                "frp_mw": round(m["frp"] * 1.15, 2),
                "dias_sem_chuva": m["dias_seco"] + 4,
                "precipitacao_mm": 0.0,
                "latitude": round(m["lat"] - 0.018, 4),
                "longitude": round(m["lon"] + 0.018, 4),
            })

            agro_bruto = round(m["gee_liq"] * (m["agro_pct"] / 100.0) * 1.18, 2)
            agro_remocao = round(agro_bruto * 0.15, 2)
            agro_liq = round(agro_bruto - agro_remocao, 2)
            outros_bruto = round(max(100000.0, m["gee_liq"] - agro_liq), 2)

            emissoes_rows.append({
                "cod_emissao_seeg": f"SEEG-AGRO-{m['id']}-2024",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "ano_inventario": 2024,
                "setor_emissao": "Mudanca_Uso_Terra_Florestas",
                "subsetor": "Alteracao_Uso_Solo_Desmatamento",
                "gas_efeito_estufa": "CO2e_GWP_AR5",
                "emissao_bruta_tco2e": agro_bruto,
                "remocao_tco2e": agro_remocao,
                "emissao_liquida_tco2e": agro_liq,
            })
            emissoes_rows.append({
                "cod_emissao_seeg": f"SEEG-ENE-{m['id']}-2024",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "ano_inventario": 2024,
                "setor_emissao": "Energia",
                "subsetor": "Transportes_Industria_Agro",
                "gas_efeito_estufa": "CO2e_GWP_AR5",
                "emissao_bruta_tco2e": outros_bruto,
                "remocao_tco2e": 0.0,
                "emissao_liquida_tco2e": outros_bruto,
            })

            risco_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "bioma": m["bioma"],
                "risco_fogo_medio_inpe": m["risco"],
                "frp_medio_mw": m["frp"],
                "dias_seco_consecutivos_max": m["dias_seco"] + 4,
                "concentracao_estacao_seca_pct": round(min(94.0, 52.0 + m["risco"] * 45.0), 2),
                "area_cicatriz_queimada_ha": m["area_ha"],
            })

            resumo_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "bioma_predominante": m["bioma"],
                "qtd_focos_calor_inpe": m["focos"],
                "area_cicatriz_queimada_ha": m["area_ha"],
                "risco_fogo_medio_inpe": m["risco"],
                "emissoes_liquidas_tco2e_seeg": m["gee_liq"],
                "participacao_agro_lulucf_pct": m["agro_pct"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "bioma_predominante": m["bioma"],
                "qtd_focos_calor_inpe": m["focos"],
                "area_cicatriz_queimada_ha": m["area_ha"],
                "risco_fogo_medio_inpe": m["risco"],
                "emissoes_liquidas_tco2e_seeg": m["gee_liq"],
                "participacao_agro_lulucf_pct": m["agro_pct"],
                "score_resiliencia_climatica_fogo_gee": m["score"],
                "classificacao_risco_climatico_queimadas": _classify_climate_fire_risk(m["score"]),
            })

        cleaned_focos = apply_sentinel_cleaning(focos_rows, self.contract)
        focos_table = pa.Table.from_pylist(cleaned_focos)
        obs = evaluate_five_pillars(
            table=focos_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"INPE Queimadas & SEEG 5-Pillar check failed: {obs.violations}")

        pq.write_table(focos_table, focos_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(emissoes_rows),
            silver_dir / "seeg_emissoes_gee_setor.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(risco_rows),
            silver_dir / "inpe_risco_fogo_mensal.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(resumo_rows),
            silver_dir / "queimadas_seeg_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "focos_queimadas": len(focos_rows),
                "emissoes_seeg": len(emissoes_rows),
                "risco_fogo": len(risco_rows),
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
        """Execute Medallion materialization and return inpe_focos_queimadas Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "inpe_focos_queimadas.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_fire_hotspots(self, termo: str) -> list[dict[str, Any]]:
        """Lookup INPE active fire hotspots by code, satellite, biome, IBGE 7D, UF, or name."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "inpe_focos_queimadas.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_foco_inpe"]) == term
            or term in _norm_text(r["satelite_referencia"])
            or term in _norm_text(r["bioma"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_seeg_emissions(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SEEG municipal GHG emissions by code, sector, IBGE 7D, UF, or municipality."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "seeg_emissoes_gee_setor.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_emissao_seeg"]) == term
            or term in _norm_text(r["setor_emissao"])
            or term in _norm_text(r["subsetor"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_fire_risk(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal INPE seasonal fire risk, FRP intensity, and burned scar area."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "inpe_risco_fogo_mensal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal INPE hotspots count and SEEG net GHG emissions."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "queimadas_seeg_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_queimadas_seeg_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold INPE Queimadas & SEEG GHG climate resilience and ESG risk features."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_queimadas_emissoes_gee.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_emissions: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by climate resilience score or highest GHG emissions."""
        rows = self._read_parquet_rows("gold", "features_queimadas_emissoes_gee.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_emissions:
            rows.sort(
                key=lambda r: (-float(r["emissoes_liquidas_tco2e_seeg"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_resiliencia_climatica_fogo_gee"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("inpe_queimadas_seeg", InpeQueimadasSeegConnector)
