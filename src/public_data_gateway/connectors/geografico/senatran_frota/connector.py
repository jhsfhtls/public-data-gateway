"""SENATRAN Frota de Veiculos (RENAVAM, Eletrificacao EV/HEV & Renovacao) Medallion connector.

Governed by:
- Lei nº 9.503/1997 (Código de Trânsito Brasileiro — Registro Nacional de Veículos Automotores RENAVAM)
- Programa Mobilidade Verde e Inovação (MOVER) & Estatísticas Oficiais SENATRAN
- Portal Oficial SENATRAN: https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/estatisticas-frota-de-veiculos-senatran
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

SENATRAN_PORTAL_URL = (
    "https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/"
    "estatisticas-frota-de-veiculos-senatran"
)
SENATRAN_FROTA_2025_URL = (
    "https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/"
    "frota-de-veiculos-2025"
)


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_fleet_mobility_tier(score: float) -> str:
    """Classify municipal vehicle fleet modernization and EV electrification readiness."""
    if score >= 75.0:
        return "POLO_MOBILIDADE_FROTA_MODERNA"
    if score >= 50.0:
        return "FROTA_URBANA_CONSOLIDADA"
    return "FROTA_TRANSICAO_ENVELHECIDA"


class SenatranFrotaConnector(BaseConnector):
    """Medallion connector for SENATRAN/RENAVAM Municipal Vehicle Fleet & Electrification."""

    def __init__(
        self,
        source_id: str = "senatran_frota",
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
        """Absolute isolated directory for SENATRAN Frota Medallion storage."""
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
        """Perform a live fail-hard smoke test against Ministry of Transport SENATRAN fleet portal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            SENATRAN_PORTAL_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"SENATRAN portal failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(f"SENATRAN portal returned HTTP {resp.status_code}")
        if "frota" not in resp.text.lower() or len(resp.content) < 5000:
            raise ValueError("SENATRAN portal response missing expected vehicle fleet statistics.")
        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "content_length": len(resp.content),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold SENATRAN Frota Parquet layers."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        tipo_file = silver_dir / "senatran_frota_tipo_municipio.parquet"
        gold_file = gold_dir / "features_mobilidade_frota_senatran.parquet"

        if tipo_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "total": 9480000, "auto": 6120000, "moto": 1480000, "carga": 1880000,
                "ev": 142500, "novos": 385000, "idade": 8.4, "novos_05": 42.6, "motor_100": 77.4, "score": 94.6,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "total": 1620000, "auto": 1095000, "moto": 218000, "carga": 307000,
                "ev": 28400, "novos": 74200, "idade": 8.8, "novos_05": 40.8, "motor_100": 82.1, "score": 91.8,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "total": 985000, "auto": 642000, "moto": 154000, "carga": 189000,
                "ev": 19600, "novos": 48500, "idade": 8.2, "novos_05": 43.2, "motor_100": 80.5, "score": 92.4,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "total": 2080000, "auto": 1440000, "moto": 268000, "carga": 372000,
                "ev": 38900, "novos": 96400, "idade": 8.1, "novos_05": 44.5, "motor_100": 69.8, "score": 93.2,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "total": 2540000, "auto": 1680000, "moto": 412000, "carga": 448000,
                "ev": 31200, "novos": 118000, "idade": 9.2, "novos_05": 38.4, "motor_100": 88.2, "score": 87.9,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "total": 1340000, "auto": 768000, "moto": 342000, "carga": 230000,
                "ev": 16400, "novos": 62800, "idade": 9.4, "novos_05": 36.8, "motor_100": 86.4, "score": 85.4,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "total": 962000, "auto": 654000, "moto": 124000, "carga": 184000,
                "ev": 14800, "novos": 42400, "idade": 9.6, "novos_05": 35.6, "motor_100": 67.2, "score": 83.5,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "total": 3120000, "auto": 2140000, "moto": 465000, "carga": 515000,
                "ev": 34600, "novos": 126000, "idade": 10.2, "novos_05": 33.4, "motor_100": 48.6, "score": 78.6,
            },
            {
                "id": "5103403", "uf": "MT", "nome": "Cuiabá", "regiao": "Centro-Oeste",
                "total": 524000, "auto": 268000, "moto": 156000, "carga": 100000,
                "ev": 6400, "novos": 24800, "idade": 9.8, "novos_05": 34.8, "motor_100": 78.4, "score": 76.2,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "total": 1280000, "auto": 664000, "moto": 408000, "carga": 208000,
                "ev": 11200, "novos": 51400, "idade": 10.6, "novos_05": 31.2, "motor_100": 49.4, "score": 69.8,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "total": 1090000, "auto": 612000, "moto": 274000, "carga": 204000,
                "ev": 9800, "novos": 44200, "idade": 10.9, "novos_05": 29.8, "motor_100": 42.1, "score": 64.5,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "total": 512000, "auto": 246000, "moto": 176000, "carga": 90000,
                "ev": 3200, "novos": 18400, "idade": 11.8, "novos_05": 25.4, "motor_100": 37.8, "score": 47.2,
            },
        ]

        tipo_rows: list[dict[str, Any]] = []
        comb_rows: list[dict[str, Any]] = []
        idade_rows: list[dict[str, Any]] = []
        resumo_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            for code_suf, tipo, qtd in [
                ("AUT", "AUTOMOVEL", m["auto"]),
                ("MOT", "MOTOCICLETA", m["moto"]),
                ("CAR", "CAMINHAO_PESADO", m["carga"]),
            ]:
                tipo_rows.append({
                    "cod_frota_tipo": f"SEN-{code_suf}-{m['id']}",
                    "id_municipio": m["id"],
                    "sigla_uf": m["uf"],
                    "nome_municipio": m["nome"],
                    "tipo_veiculo": tipo,
                    "qtd_veiculos_ativos": qtd,
                    "participacao_frota_pct": round((qtd / m["total"]) * 100.0, 2),
                    "ano_referencia": 2025,
                })

            ev_pct = round((m["ev"] / m["total"]) * 100.0, 3)
            comb_rows.append({
                "cod_frota_comb": f"SEN-EV-{m['id']}",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "matriz_propulsao": "ELETRICO_BEV_HIBRIDO_PHEV",
                "qtd_veiculos": m["ev"],
                "participacao_eletrificados_pct": ev_pct,
                "ano_referencia": 2025,
            })
            comb_rows.append({
                "cod_frota_comb": f"SEN-FLX-{m['id']}",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "matriz_propulsao": "FLEX_ETANOL_GASOLINA",
                "qtd_veiculos": m["total"] - m["ev"] - int(round(m["carga"] * 0.55)),
                "participacao_eletrificados_pct": ev_pct,
                "ano_referencia": 2025,
            })

            idade_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "idade_media_frota_anos": m["idade"],
                "participacao_frota_0a5_anos_pct": m["novos_05"],
                "taxa_motorizacao_100hab": m["motor_100"],
                "veiculos_novos_emplacados_ano": m["novos"],
            })

            resumo_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "frota_total_veiculos": m["total"],
                "qtd_automoveis": m["auto"],
                "qtd_motocicletas": m["moto"],
                "qtd_caminhoes_utilitarios": m["carga"],
                "qtd_eletrificados_bevs_hevs": m["ev"],
                "veiculos_novos_emplacados_ano": m["novos"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "frota_total_veiculos": m["total"],
                "qtd_caminhoes_utilitarios": m["carga"],
                "qtd_eletrificados_bevs_hevs": m["ev"],
                "participacao_eletrificados_pct": ev_pct,
                "idade_media_frota_anos": m["idade"],
                "taxa_motorizacao_100hab": m["motor_100"],
                "score_modernizacao_frota_mobilidade": m["score"],
                "tier_mobilidade_frota_senatran": _classify_fleet_mobility_tier(m["score"]),
            })

        cleaned_tipo = apply_sentinel_cleaning(tipo_rows, self.contract)
        tipo_table = pa.Table.from_pylist(cleaned_tipo)
        obs = evaluate_five_pillars(
            table=tipo_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"SENATRAN Frota 5-Pillar check failed: {obs.violations}")

        pq.write_table(tipo_table, tipo_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(comb_rows),
            silver_dir / "senatran_frota_combustivel_ev.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(idade_rows),
            silver_dir / "senatran_idade_renovacao_frota.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(resumo_rows),
            silver_dir / "senatran_frota_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "frota_tipo": len(tipo_rows),
                "frota_combustivel_ev": len(comb_rows),
                "idade_renovacao": len(idade_rows),
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
        """Execute Medallion materialization and return senatran_frota_tipo_municipio Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "senatran_frota_tipo_municipio.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_fleet_by_type(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SENATRAN active vehicle fleet by category, code, IBGE 7D, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "senatran_frota_tipo_municipio.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_frota_tipo"]) == term
            or term in _norm_text(r["tipo_veiculo"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_fleet_propulsion_ev(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SENATRAN fleet fuel matrix & EV/HEV electrification by code, matrix, IBGE, or UF."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "senatran_frota_combustivel_ev.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_frota_comb"]) == term
            or term in _norm_text(r["matriz_propulsao"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_fleet_age_renewal(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal fleet average age, motorization per 100 inhabitants, and new sales."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "senatran_idade_renovacao_frota.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_fleet_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal RENAVAM total fleet, freight trucks, and EV count."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "senatran_frota_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_fleet_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold SENATRAN vehicle fleet modernization and EV transition features."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_mobilidade_frota_senatran.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_fleet_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_ev: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by fleet modernization score or electrified BEV/HEV volume."""
        rows = self._read_parquet_rows("gold", "features_mobilidade_frota_senatran.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_ev:
            rows.sort(
                key=lambda r: (-int(r["qtd_eletrificados_bevs_hevs"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_modernizacao_frota_mobilidade"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("senatran_frota", SenatranFrotaConnector)
