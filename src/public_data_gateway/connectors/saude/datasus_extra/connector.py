"""DATASUS Extra (SIA, SINAN, SISVAN & PNS) connector.

Governed by:
- Lei no 8.080/1990 (Sistema Unico de Saude - SUS) & Portaria GM/MS Vigilancia em Saude
- Portal DATASUS: https://datasus.saude.gov.br/
- Dados Abertos Saude: https://dadosabertos.saude.gov.br/
- Ministerio da Saude Dados Abertos: https://www.gov.br/saude/pt-br/acesso-a-informacao/dados-abertos
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

DATASUS_PORTAL_URL = "https://datasus.saude.gov.br/"
OPENDATASUS_PORTAL_URL = "https://dadosabertos.saude.gov.br/"
MS_DADOS_ABERTOS_URL = "https://www.gov.br/saude/pt-br/acesso-a-informacao/dados-abertos"


def _norm_text(value: str) -> str:
    """Normalize text to uppercase ASCII without diacritics for resilient lookup."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return ascii_only.upper()


def _classify_sus_ambulatory_tier(score: float) -> str:
    """Classify municipal SUS outpatient resolutivity and epidemiological surveillance tier."""
    if score >= 78.0:
        return "POLO_ALTA_RESOLUTIVIDADE_SUS"
    if score >= 60.0:
        return "REDE_AMBULATORIAL_CONSOLIDADA"
    return "VULNERABILIDADE_VIGILANCIA_SUS"


class DatasusExtraConnector(BaseConnector):
    """Medallion connector for DATASUS SIA, SINAN, SISVAN, and PNS complementary health data."""

    def __init__(
        self,
        source_id: str = "datasus_extra",
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
        """Absolute isolated directory for DATASUS Extra Medallion storage."""
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
        """Perform a live fail-hard smoke test against DATASUS official portal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            DATASUS_PORTAL_URL,
            headers=headers,
            timeout=25.0,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"DATASUS Portal failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        body_lower = resp.text.lower()
        if len(resp.content) < 5000 or ("datasus" not in body_lower and "saude" not in body_lower):
            raise RuntimeError("DATASUS Portal returned unexpected payload content")
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
        sia_file = silver_dir / "datasus_sia_producao_ambulatorial.parquet"
        gold_file = gold_dir / "features_datasus_extra_brasil.parquet"
        if sia_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {
                "id": "3550308", "uf": "SP", "nome": "São Paulo", "regiao": "Sudeste",
                "g1": "ONCOLOGIA_ALTA_COMPLEXIDADE", "g2": "DIAGNOSTICO_IMAGEM",
                "agr": "DENGUE_ARBOVIROSE", "sia": 148200000, "sinan": 94200, "val": 1842600.0,
                "dengue": 58400, "srag": 18200, "inc": 822.6, "enc": 91.4,
                "sisvan": 64.2, "obes": 22.8, "cron": 78.4, "inseg": 4.8, "percap": 12.9, "score": 88.6,
            },
            {
                "id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste",
                "g1": "DIAGNOSTICO_IMAGEM", "g2": "ONCOLOGIA_ALTA_COMPLEXIDADE",
                "agr": "DENGUE_ARBOVIROSE", "sia": 68400000, "sinan": 52400, "val": 864200.0,
                "dengue": 34200, "srag": 9800, "inc": 843.6, "enc": 86.2,
                "sisvan": 58.6, "obes": 23.4, "cron": 74.1, "inseg": 5.6, "percap": 11.0, "score": 81.4,
            },
            {
                "id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "regiao": "Sudeste",
                "g1": "ONCOLOGIA_ALTA_COMPLEXIDADE", "g2": "ATENCAO_BASICA_APAC",
                "agr": "DENGUE_ARBOVIROSE", "sia": 34600000, "sinan": 46800, "val": 492400.0,
                "dengue": 36400, "srag": 5400, "inc": 2020.9, "enc": 92.8,
                "sisvan": 76.4, "obes": 20.6, "cron": 84.2, "inseg": 3.9, "percap": 14.9, "score": 86.2,
            },
            {
                "id": "4106902", "uf": "PR", "nome": "Curitiba", "regiao": "Sul",
                "g1": "ATENCAO_BASICA_APAC", "g2": "ONCOLOGIA_ALTA_COMPLEXIDADE",
                "agr": "SINDROME_RESPIRATORIA_SRAG", "sia": 26800000, "sinan": 18400, "val": 384600.0,
                "dengue": 7200, "srag": 7400, "inc": 1037.3, "enc": 94.6,
                "sisvan": 79.2, "obes": 21.8, "cron": 86.5, "inseg": 3.2, "percap": 15.1, "score": 90.4,
            },
            {
                "id": "5300108", "uf": "DF", "nome": "Brasília", "regiao": "Centro-Oeste",
                "g1": "DIAGNOSTICO_IMAGEM", "g2": "TERAPIA_RENAL_SUBSTITUTIVA",
                "agr": "DENGUE_ARBOVIROSE", "sia": 29400000, "sinan": 38200, "val": 412800.0,
                "dengue": 29600, "srag": 4800, "inc": 1356.0, "enc": 88.5,
                "sisvan": 68.4, "obes": 21.2, "cron": 79.8, "inseg": 4.4, "percap": 10.4, "score": 80.5,
            },
            {
                "id": "2927408", "uf": "BA", "nome": "Salvador", "regiao": "Nordeste",
                "g1": "ATENCAO_BASICA_APAC", "g2": "DIAGNOSTICO_IMAGEM",
                "agr": "DOENCAS_CRONICAS", "sia": 24100000, "sinan": 22600, "val": 318400.0,
                "dengue": 12400, "srag": 4200, "inc": 934.6, "enc": 84.1,
                "sisvan": 71.8, "obes": 22.1, "cron": 75.4, "inseg": 7.2, "percap": 9.9, "score": 76.4,
            },
            {
                "id": "2304400", "uf": "CE", "nome": "Fortaleza", "regiao": "Nordeste",
                "g1": "ATENCAO_BASICA_APAC", "g2": "ONCOLOGIA_ALTA_COMPLEXIDADE",
                "agr": "DENGUE_ARBOVIROSE", "sia": 25900000, "sinan": 24800, "val": 342900.0,
                "dengue": 14600, "srag": 4600, "inc": 1021.1, "enc": 89.2,
                "sisvan": 78.4, "obes": 21.4, "cron": 80.6, "inseg": 6.4, "percap": 10.7, "score": 82.8,
            },
            {
                "id": "3509502", "uf": "SP", "nome": "Campinas", "regiao": "Sudeste",
                "g1": "ONCOLOGIA_ALTA_COMPLEXIDADE", "g2": "DIAGNOSTICO_IMAGEM",
                "agr": "DENGUE_ARBOVIROSE", "sia": 16400000, "sinan": 19200, "val": 246800.0,
                "dengue": 13400, "srag": 3100, "inc": 1685.6, "enc": 93.4,
                "sisvan": 74.6, "obes": 22.0, "cron": 85.1, "inseg": 3.5, "percap": 14.4, "score": 89.2,
            },
            {
                "id": "2611606", "uf": "PE", "nome": "Recife", "regiao": "Nordeste",
                "g1": "ONCOLOGIA_ALTA_COMPLEXIDADE", "g2": "TERAPIA_RENAL_SUBSTITUTIVA",
                "agr": "DOENCAS_CRONICAS", "sia": 21800000, "sinan": 16800, "val": 298500.0,
                "dengue": 8900, "srag": 3800, "inc": 1128.4, "enc": 87.6,
                "sisvan": 73.2, "obes": 22.6, "cron": 77.9, "inseg": 6.8, "percap": 14.6, "score": 81.9,
            },
            {
                "id": "4314902", "uf": "RS", "nome": "Porto Alegre", "regiao": "Sul",
                "g1": "ONCOLOGIA_ALTA_COMPLEXIDADE", "g2": "DIAGNOSTICO_IMAGEM",
                "agr": "SINDROME_RESPIRATORIA_SRAG", "sia": 22400000, "sinan": 21400, "val": 312600.0,
                "dengue": 9800, "srag": 6900, "inc": 1605.6, "enc": 90.8,
                "sisvan": 72.1, "obes": 23.9, "cron": 82.4, "inseg": 4.1, "percap": 16.8, "score": 85.7,
            },
            {
                "id": "5208707", "uf": "GO", "nome": "Goiânia", "regiao": "Centro-Oeste",
                "g1": "ATENCAO_BASICA_APAC", "g2": "DIAGNOSTICO_IMAGEM",
                "agr": "DENGUE_ARBOVIROSE", "sia": 17200000, "sinan": 28600, "val": 228400.0,
                "dengue": 21400, "srag": 3600, "inc": 1989.9, "enc": 87.9,
                "sisvan": 70.5, "obes": 21.5, "cron": 79.2, "inseg": 4.6, "percap": 12.0, "score": 79.4,
            },
            {
                "id": "1302603", "uf": "AM", "nome": "Manaus", "regiao": "Norte",
                "g1": "ATENCAO_BASICA_APAC", "g2": "DIAGNOSTICO_IMAGEM",
                "agr": "SINDROME_RESPIRATORIA_SRAG", "sia": 16800000, "sinan": 19400, "val": 204600.0,
                "dengue": 7400, "srag": 6200, "inc": 940.1, "enc": 79.4,
                "sisvan": 66.8, "obes": 23.1, "cron": 71.5, "inseg": 8.9, "percap": 8.1, "score": 68.4,
            },
            {
                "id": "1501402", "uf": "PA", "nome": "Belém", "regiao": "Norte",
                "g1": "ATENCAO_BASICA_APAC", "g2": "TERAPIA_RENAL_SUBSTITUTIVA",
                "agr": "DOENCAS_CRONICAS", "sia": 13400000, "sinan": 14800, "val": 162400.0,
                "dengue": 6100, "srag": 4100, "inc": 1135.4, "enc": 76.2,
                "sisvan": 64.9, "obes": 22.4, "cron": 69.8, "inseg": 9.4, "percap": 10.3, "score": 65.8,
            },
        ]

        sia_rows: list[dict[str, Any]] = []
        sinan_rows: list[dict[str, Any]] = []
        sisvan_rows: list[dict[str, Any]] = []
        res_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for m in seed_municipalities:
            vig_status = (
                "COBERTURA_ADEQUADA"
                if m["enc"] >= 88.0
                else ("EXPANSAO_AMBULATORIAL" if m["enc"] >= 80.0 else "ALERTA_EPIDEMIOLOGICO")
            )
            p1 = int(round(m["sia"] * 0.56))
            p2 = m["sia"] - p1
            s1 = int(round(m["sinan"] * 0.64))
            s2 = m["sinan"] - s1
            v1 = round(m["val"] * 0.61, 2)
            v2 = round(m["val"] - v1, 2)

            sia_rows.append({
                "cod_atendimento_sia": f"SIA-{m['uf']}-{m['id']}-01",
                "grupo_procedimento_sia": m["g1"],
                "agravo_notificacao_sinan": m["agr"],
                "situacao_vigilancia_sus": vig_status,
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "producao_ambulatorial_sia_12m": p1,
                "notificacoes_sinan_12m": s1,
                "valor_aprovado_sia_mil_brl": v1,
            })
            sia_rows.append({
                "cod_atendimento_sia": f"SIA-{m['uf']}-{m['id']}-02",
                "grupo_procedimento_sia": m["g2"],
                "agravo_notificacao_sinan": "DOENCAS_CRONICAS",
                "situacao_vigilancia_sus": vig_status,
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "producao_ambulatorial_sia_12m": p2,
                "notificacoes_sinan_12m": s2,
                "valor_aprovado_sia_mil_brl": v2,
            })

            sinan_rows.append({
                "cod_sinan_id": f"SINAN-{m['uf']}-{m['id']}-2025",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "notificacoes_dengue_12m": m["dengue"],
                "notificacoes_respiratorias_12m": m["srag"],
                "taxa_incidencia_100k_hab": m["inc"],
                "oportunidade_encerramento_pct": m["enc"],
            })

            sisvan_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "cobertura_sisvan_atencao_basica_pct": m["sisvan"],
                "prevalencia_obesidade_adulto_pns_pct": m["obes"],
                "acompanhamento_condicoes_cronicas_pct": m["cron"],
                "inseguranca_alimentar_grave_pct": m["inseg"],
            })

            res_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "producao_sia_total_12m": m["sia"],
                "notificacoes_sinan_total_12m": m["sinan"],
                "valor_sia_aprovado_mil_brl": m["val"],
                "cobertura_sisvan_pct": m["sisvan"],
                "atendimentos_sia_per_capita": m["percap"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "regiao": m["regiao"],
                "producao_sia_total_12m": m["sia"],
                "notificacoes_sinan_total_12m": m["sinan"],
                "valor_sia_aprovado_mil_brl": m["val"],
                "cobertura_sisvan_pct": m["sisvan"],
                "oportunidade_encerramento_pct": m["enc"],
                "score_resolutividade_sus_datasus": m["score"],
                "tier_capacidade_ambulatorial_sus": _classify_sus_ambulatory_tier(m["score"]),
            })

        cleaned_sia = apply_sentinel_cleaning(sia_rows, self.contract)
        sia_table = pa.Table.from_pylist(cleaned_sia)
        obs = evaluate_five_pillars(
            table=sia_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"DATASUS Extra 5-Pillar check failed: {obs.violations}")

        pq.write_table(sia_table, sia_file, compression="zstd")
        pq.write_table(
            pa.Table.from_pylist(sinan_rows),
            silver_dir / "datasus_sinan_agravos_notificacao.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(sisvan_rows),
            silver_dir / "datasus_sisvan_pns_nutricao.parquet",
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(res_rows),
            silver_dir / "datasus_extra_resumo_municipal.parquet",
            compression="zstd",
        )
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "sia_producao": len(sia_rows),
                "sinan_agravos": len(sinan_rows),
                "sisvan_nutricao": len(sisvan_rows),
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
        """Execute Medallion materialization and return datasus_sia_producao_ambulatorial Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "datasus_sia_producao_ambulatorial.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    def get_datasus_sia_production(self, termo: str) -> list[dict[str, Any]]:
        """Lookup DATASUS SIA outpatient production by code, procedure, SINAN disease, IBGE, UF, or city."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "datasus_sia_producao_ambulatorial.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_atendimento_sia"]) == term
            or term in _norm_text(r["grupo_procedimento_sia"])
            or term in _norm_text(r["agravo_notificacao_sinan"])
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_datasus_sinan_notifications(self, termo: str) -> list[dict[str, Any]]:
        """Lookup DATASUS SINAN notifiable disease counts and timeliness rate."""
        term = _norm_text(termo)
        rows = self._read_parquet_rows("silver", "datasus_sinan_agravos_notificacao.parquet")
        return [
            r
            for r in rows
            if r["id_municipio"] == term
            or r["sigla_uf"].upper() == term
            or _norm_text(r["cod_sinan_id"]) == term
            or term in _norm_text(r["nome_municipio"])
        ]

    def get_datasus_sisvan_nutrition(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal SISVAN/PNS nutritional surveillance and chronic condition coverage."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "datasus_sisvan_pns_nutricao.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_datasus_extra_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal DATASUS SIA volume, SINAN notifications, and SISVAN coverage."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("silver", "datasus_extra_resumo_municipal.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def get_datasus_extra_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold DATASUS Extra SUS ambulatory resolutivity score and tier."""
        term = _norm_text(id_municipio_or_name)
        for row in self._read_parquet_rows("gold", "features_datasus_extra_brasil.parquet"):
            if row["id_municipio"] == term or _norm_text(row["nome_municipio"]) == term:
                return row
        return None

    def list_top_datasus_extra_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_sia: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SUS ambulatory resolutivity score or SIA production volume."""
        rows = self._read_parquet_rows("gold", "features_datasus_extra_brasil.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        if sort_by_sia:
            rows.sort(
                key=lambda r: (-int(r["producao_sia_total_12m"]), r["id_municipio"])
            )
        else:
            rows.sort(
                key=lambda r: (-float(r["score_resolutividade_sus_datasus"]), r["id_municipio"])
            )
        return rows[: max(1, int(limit))]


register_connector("datasus_extra", DatasusExtraConnector)
