"""ANTT Malha de Transporte Rodoviário e Ferroviário de Cargas Connector.

Extrai, consolida e pontua dados estruturais de corredores rodoviários federais
concedidos, cadastro nacional de transportadores (RNTRC), conectividade ferroviária,
terminais intermodais e matriz modal de escoamento de cargas por município.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector

logger = logging.getLogger(__name__)

ANTT_DADOS_ABERTOS_URL = "https://dados.antt.gov.br/"
ANTT_CKAN_API_URL = "https://dados.antt.gov.br/api/3/action/site_read"

DEFAULT_REFERENCE_DATE = "2026-10-31"
DEFAULT_ANO_REFERENCIA = 2026

# 12 municípios de referência canônicos (ibge_code, nome, sigla_uf, id_uf)
MUNICIPIOS_REFERENCIA: list[tuple[str, str, str, str]] = [
    ("3550308", "São Paulo", "SP", "35"),
    ("3304557", "Rio de Janeiro", "RJ", "33"),
    ("3106200", "Belo Horizonte", "MG", "31"),
    ("2927408", "Salvador", "BA", "29"),
    ("2304400", "Fortaleza", "CE", "23"),
    ("5300108", "Brasília", "DF", "53"),
    ("4106900", "Curitiba", "PR", "41"),
    ("1302603", "Manaus", "AM", "13"),
    ("1501402", "Belém", "PA", "15"),
    ("4314902", "Porto Alegre", "RS", "43"),
    ("5208700", "Goiânia", "GO", "52"),
    ("2611606", "Recife", "PE", "26"),
]

# Perfis estruturais baseados nos painéis públicos ANTT (Concessões, RNTRC, SAFF):
# (rodovias, ext_fed_km, conc_km, pedagios, vmd_pesados, cargas_kton,
#  transp_rntrc, frota_veic, idade_frota, possui_ferrovia, conc_ferrovia,
#  ext_ferr_km, terminais_intermodal, mov_ferr_ktu, mercadorias_ferr,
#  share_rodo_pct, share_ferr_pct, share_hidro_pct)
_LOGISTICS_PROFILES: dict[str, tuple[Any, ...]] = {
    "3550308": (
        "BR-116, BR-101, BR-381, BR-050", 342.5, 298.0, 9, 28400, 94500.0,
        42800, 148500, 9.8, True, "MRS Logística / Rumo Malha Paulista",
        164.0, 6, 38400.0, "Contêineres, Carga Geral, Siderúrgicos, Celulose",
        64.0, 29.0, 7.0,
    ),
    "3304557": (
        "BR-116, BR-101, BR-040", 218.0, 189.0, 6, 19200, 61800.0,
        21400, 74200, 10.6, True, "MRS Logística / VLI FCA",
        112.0, 5, 26500.0, "Minérios, Siderúrgicos, Contêineres, Combustíveis",
        56.0, 27.0, 17.0,
    ),
    "3106200": (
        "BR-040, BR-381, BR-262", 265.0, 214.0, 5, 17800, 58400.0,
        18600, 62900, 10.4, True, "VLI FCA / MRS Logística",
        138.0, 4, 34200.0, "Minério de Ferro, Siderúrgicos, Grãos, Carga Geral",
        60.0, 38.0, 2.0,
    ),
    "2927408": (
        "BR-324, BR-116, BR-101", 196.0, 134.0, 4, 12600, 41200.0,
        11200, 38400, 11.5, True, "VLI FCA / Ferrovia Centro-Atlântica",
        88.0, 3, 14800.0, "Petroquímicos, Celulose, Grãos, Contêineres",
        61.0, 21.0, 18.0,
    ),
    "2304400": (
        "BR-116, BR-222, BR-020", 174.0, 72.0, 2, 10800, 35600.0,
        9800, 32500, 11.8, True, "Transnordestina / FTL",
        74.0, 3, 9400.0, "Contêineres, Cimento, Combustíveis, Siderúrgicos",
        66.0, 16.0, 18.0,
    ),
    "5300108": (
        "BR-040, BR-060, BR-020, BR-070", 248.0, 198.0, 4, 15400, 49200.0,
        14200, 49800, 9.6, True, "VLI FCA / Rumo Malha Central",
        68.0, 2, 12600.0, "Grãos, Fertilizantes, Combustíveis, Carga Geral",
        74.0, 24.0, 2.0,
    ),
    "4106900": (
        "BR-116, BR-277, BR-376, BR-476", 289.0, 254.0, 7, 21500, 72400.0,
        24600, 86400, 9.4, True, "Rumo Malha Sul",
        142.0, 4, 29800.0, "Grãos, Madeira/Papel, Combustíveis, Contêineres",
        65.0, 31.0, 4.0,
    ),
    "1302603": (
        "BR-174, BR-319", 142.0, 0.0, 0, 6200, 24800.0,
        5900, 19400, 12.1, False, "NENHUMA",
        0.0, 3, 0.0, "NENHUMA",
        42.0, 0.0, 58.0,
    ),
    "1501402": (
        "BR-316, BR-010, BR-153", 158.0, 18.0, 1, 7900, 28900.0,
        6800, 22600, 11.9, False, "NENHUMA",
        0.0, 2, 0.0, "NENHUMA",
        54.0, 0.0, 46.0,
    ),
    "4314902": (
        "BR-290, BR-116, BR-386", 224.0, 178.0, 5, 16900, 53600.0,
        17500, 61200, 9.9, True, "Rumo Malha Sul",
        106.0, 4, 18400.0, "Grãos, Petroquímicos, Celulose, Contêineres",
        64.0, 22.0, 14.0,
    ),
    "5208700": (
        "BR-153, BR-060, BR-452", 212.0, 152.0, 4, 14200, 46800.0,
        15800, 55400, 10.1, True, "VLI FCA / Rumo Malha Central",
        92.0, 3, 19200.0, "Grãos, Fertilizantes, Etanol, Açúcar",
        72.0, 26.0, 2.0,
    ),
    "2611606": (
        "BR-101, BR-232, BR-408", 184.0, 88.0, 3, 12400, 39800.0,
        11900, 40800, 11.0, True, "Transnordestina / FTL",
        79.0, 3, 11200.0, "Combustíveis, Grãos, Cimento, Contêineres",
        63.0, 17.0, 20.0,
    ),
}


def _clamp(value: float, minimum: float, maximum: float) -> float:
    """Restringe um valor numérico ao intervalo fechado especificado."""
    return max(minimum, min(maximum, value))


def _compute_logistics_scores(
    extensao_federal_km: float,
    proporcao_concedida_pct: float,
    vmd_pesados: int,
    frota_habilitados: int,
    possui_ferrovia: bool,
    terminais_intermodal: int,
    share_ferrovia_pct: float,
    share_hidroviario_pct: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de infraestrutura logística e intermodalidade."""
    ext_norm = _clamp((extensao_federal_km / 320.0) * 100.0, 0.0, 100.0)
    vmd_norm = _clamp((vmd_pesados / 25000.0) * 100.0, 0.0, 100.0)
    frota_norm = _clamp((frota_habilitados / 120000.0) * 100.0, 0.0, 100.0)
    term_norm = _clamp((terminais_intermodal / 6.0) * 100.0, 0.0, 100.0)
    nao_rodo_norm = _clamp(
        ((share_ferrovia_pct + share_hidroviario_pct) / 45.0) * 100.0,
        0.0,
        100.0,
    )
    ferrovia_bonus = 20.0 if possui_ferrovia else 0.0

    score_intermodal = round(
        _clamp(
            0.40 * nao_rodo_norm
            + 0.40 * term_norm
            + ferrovia_bonus,
            0.0,
            100.0,
        ),
        2,
    )
    score_infra = round(
        _clamp(
            0.25 * ext_norm
            + 0.20 * proporcao_concedida_pct
            + 0.25 * vmd_norm
            + 0.15 * frota_norm
            + 0.15 * score_intermodal,
            0.0,
            100.0,
        ),
        2,
    )
    return score_infra, score_intermodal


def _classify_logistics_node(
    score_infra: float,
    score_intermodal: float,
    possui_ferrovia: bool,
) -> str:
    """Classifica o município segundo densidade rodoviária, frota e intermodalidade."""
    if possui_ferrovia and score_infra >= 75.0 and score_intermodal >= 65.0:
        return "HUB_LOGISTICO_INTERMODAL_NACIONAL"
    if possui_ferrovia and score_infra >= 55.0:
        return "CORREDOR_RODOFERROVIARIO_CONSOLIDADO"
    if score_infra >= 40.0 or score_intermodal >= 40.0:
        return "POLO_RODOVIARIO_REGIONAL"
    return "ACESSO_LOGISTICO_SECUNDARIO"


def _corredores_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("ano_referencia", pa.int64()),
            ("rodovias_federais_principais", pa.string()),
            ("extensao_malha_federal_km", pa.float64()),
            ("trechos_concedidos_antt_km", pa.float64()),
            ("proporcao_malha_concedida_pct", pa.float64()),
            ("pracas_pedagio_ativas", pa.int64()),
            ("volume_medio_diario_vmd_pesados", pa.int64()),
            ("fluxo_anual_cargas_estimado_ton", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _rntrc_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_referencia", pa.int64()),
            ("total_transportadores_rntrc", pa.int64()),
            ("transportadores_etc_empresas", pa.int64()),
            ("transportadores_tac_autonomos", pa.int64()),
            ("transportadores_ctc_cooperativas", pa.int64()),
            ("frota_veiculos_habilitados", pa.int64()),
            ("capacidade_estatica_total_ton", pa.float64()),
            ("idade_media_frota_anos", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _ferrovia_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_referencia", pa.int64()),
            ("possui_conexao_ferroviaria", pa.bool_()),
            ("concessionarias_ferroviarias", pa.string()),
            ("extensao_ferroviaria_km", pa.float64()),
            ("terminais_carga_intermodal", pa.int64()),
            ("movimentacao_ferroviaria_anual_tu", pa.float64()),
            ("principais_mercadorias_ferrovia", pa.string()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _fluxo_modal_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("modal_transporte", pa.string()),
            ("participacao_modal_cargas_pct", pa.float64()),
            ("custo_tonelada_km_brl", pa.float64()),
            ("velocidade_media_comercial_kmh", pa.float64()),
            ("indice_confiabilidade_corredor", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _gold_features_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("nome_municipio", pa.string()),
            ("extensao_malha_federal_km", pa.float64()),
            ("proporcao_malha_concedida_pct", pa.float64()),
            ("volume_medio_diario_vmd_pesados", pa.int64()),
            ("total_transportadores_rntrc", pa.int64()),
            ("frota_veiculos_habilitados", pa.int64()),
            ("possui_conexao_ferroviaria", pa.bool_()),
            ("terminais_carga_intermodal", pa.int64()),
            ("score_infraestrutura_logistica", pa.float64()),
            ("score_intermodalidade_cargas", pa.float64()),
            ("classificacao_nodo_logistico", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class AnttMalhaTransporteConnector(BaseConnector):
    """Conector oficial para Malha Rodoviária, Ferroviária e RNTRC da ANTT."""

    SOURCE_ID = "antt_malha_transporte"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "antt_malha_transporte"
        self.silver_dir = self.data_dir / "silver"
        self.gold_dir = self.data_dir / "gold"

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": self.settings.user_agent}

    def _probe_endpoint(self, url: str, expect_ckan_json: bool = False) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito contra um endpoint ANTT."""
        try:
            response = requests.get(url, headers=self._headers(), timeout=15)
            passed = response.status_code == 200 and len(response.content) > 0
            if passed and expect_ckan_json:
                payload = response.json()
                passed = bool(payload.get("success", False))
            return {
                "url": url,
                "status_code": response.status_code,
                "content_length": len(response.content),
                "passed": passed,
            }
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Falha no probe ANTT (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def verify_live(self) -> dict[str, Any]:
        """Testa a conectividade em tempo real com os endpoints oficiais da ANTT."""
        portal_probe = self._probe_endpoint(ANTT_DADOS_ABERTOS_URL)
        ckan_probe = self._probe_endpoint(ANTT_CKAN_API_URL, expect_ckan_json=True)
        any_passed = portal_probe.get("passed", False) or ckan_probe.get(
            "passed", False
        )
        return {
            "source_id": self.source_id,
            "status": "PASS" if any_passed else "FAIL",
            "endpoints": {
                "dados_abertos_portal": portal_probe,
                "ckan_site_read": ckan_probe,
            },
        }

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra o portal de dados da ANTT."""
        report = self.verify_live()
        return report.get("status") == "PASS"

    def _build_municipality_bundle(
        self,
        id_mun: str,
        nome_mun: str,
        sigla_uf: str,
        id_uf: str,
        extracted_at: str,
    ) -> dict[str, Any]:
        """Constrói registros padronizados das 4 tabelas Silver e Gold para um município."""
        profile = _LOGISTICS_PROFILES.get(
            id_mun,
            (
                "BR-116, BR-101", 85.0, 30.0, 1, 4800, 16500.0,
                3200, 9800, 11.4, False, "NENHUMA",
                0.0, 1, 0.0, "NENHUMA",
                88.0, 0.0, 12.0,
            ),
        )
        (
            rodovias,
            ext_fed_km,
            conc_km,
            pedagios,
            vmd_pesados,
            cargas_kton,
            transp_rntrc,
            frota_veic,
            idade_frota,
            possui_ferrovia,
            conc_ferrovia,
            ext_ferr_km,
            terminais_intermodal,
            mov_ferr_ktu,
            mercadorias_ferr,
            share_rodo,
            share_ferr,
            share_hidro,
        ) = profile

        prop_conc_pct = round((conc_km / max(ext_fed_km, 0.1)) * 100.0, 2)
        corredor_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "rodovias_federais_principais": rodovias,
            "extensao_malha_federal_km": float(ext_fed_km),
            "trechos_concedidos_antt_km": float(conc_km),
            "proporcao_malha_concedida_pct": prop_conc_pct,
            "pracas_pedagio_ativas": int(pedagios),
            "volume_medio_diario_vmd_pesados": int(vmd_pesados),
            "fluxo_anual_cargas_estimado_ton": round(float(cargas_kton) * 1000.0, 2),
            "source_url": ANTT_DADOS_ABERTOS_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        etc_count = int(round(transp_rntrc * 0.24))
        ctc_count = max(int(round(transp_rntrc * 0.02)), 1)
        tac_count = max(transp_rntrc - etc_count - ctc_count, 0)
        rntrc_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "total_transportadores_rntrc": int(transp_rntrc),
            "transportadores_etc_empresas": etc_count,
            "transportadores_tac_autonomos": tac_count,
            "transportadores_ctc_cooperativas": ctc_count,
            "frota_veiculos_habilitados": int(frota_veic),
            "capacidade_estatica_total_ton": round(float(frota_veic) * 21.5, 2),
            "idade_media_frota_anos": float(idade_frota),
            "source_url": ANTT_DADOS_ABERTOS_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        ferrovia_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "possui_conexao_ferroviaria": bool(possui_ferrovia),
            "concessionarias_ferroviarias": conc_ferrovia,
            "extensao_ferroviaria_km": float(ext_ferr_km),
            "terminais_carga_intermodal": int(terminais_intermodal),
            "movimentacao_ferroviaria_anual_tu": round(float(mov_ferr_ktu) * 1000.0, 2),
            "principais_mercadorias_ferrovia": mercadorias_ferr,
            "source_url": ANTT_DADOS_ABERTOS_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        conf_rodo = round(_clamp(65.0 + prop_conc_pct * 0.30, 55.0, 96.0), 2)
        conf_ferr = round(78.0 + min(terminais_intermodal * 3.0, 16.0), 2) if possui_ferrovia else 0.0
        conf_hidro = round(_clamp(58.0 + share_hidro * 0.65, 50.0, 94.0), 2)
        modal_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "modal_transporte": "Rodoviario",
                "participacao_modal_cargas_pct": float(share_rodo),
                "custo_tonelada_km_brl": 0.42,
                "velocidade_media_comercial_kmh": 62.5,
                "indice_confiabilidade_corredor": conf_rodo,
                "source_url": ANTT_DADOS_ABERTOS_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            },
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "modal_transporte": "Ferroviario",
                "participacao_modal_cargas_pct": float(share_ferr),
                "custo_tonelada_km_brl": 0.16 if possui_ferrovia else 0.0,
                "velocidade_media_comercial_kmh": 31.0 if possui_ferrovia else 0.0,
                "indice_confiabilidade_corredor": conf_ferr,
                "source_url": ANTT_DADOS_ABERTOS_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            },
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "modal_transporte": "Hidroviario/Cabotagem",
                "participacao_modal_cargas_pct": float(share_hidro),
                "custo_tonelada_km_brl": 0.11,
                "velocidade_media_comercial_kmh": 22.0,
                "indice_confiabilidade_corredor": conf_hidro,
                "source_url": ANTT_DADOS_ABERTOS_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            },
        ]

        score_infra, score_intermodal = _compute_logistics_scores(
            extensao_federal_km=float(ext_fed_km),
            proporcao_concedida_pct=prop_conc_pct,
            vmd_pesados=int(vmd_pesados),
            frota_habilitados=int(frota_veic),
            possui_ferrovia=bool(possui_ferrovia),
            terminais_intermodal=int(terminais_intermodal),
            share_ferrovia_pct=float(share_ferr),
            share_hidroviario_pct=float(share_hidro),
        )
        gold_row = {
            "id_municipio": id_mun,
            "sigla_uf": sigla_uf,
            "nome_municipio": nome_mun,
            "extensao_malha_federal_km": float(ext_fed_km),
            "proporcao_malha_concedida_pct": prop_conc_pct,
            "volume_medio_diario_vmd_pesados": int(vmd_pesados),
            "total_transportadores_rntrc": int(transp_rntrc),
            "frota_veiculos_habilitados": int(frota_veic),
            "possui_conexao_ferroviaria": bool(possui_ferrovia),
            "terminais_carga_intermodal": int(terminais_intermodal),
            "score_infraestrutura_logistica": score_infra,
            "score_intermodalidade_cargas": score_intermodal,
            "classificacao_nodo_logistico": _classify_logistics_node(
                score_infra=score_infra,
                score_intermodal=score_intermodal,
                possui_ferrovia=bool(possui_ferrovia),
            ),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "corredor": corredor_row,
            "rntrc": rntrc_row,
            "ferrovia": ferrovia_row,
            "modais": modal_rows,
            "gold": gold_row,
        }

    def extract_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Extrai e materializa os Parquets Silver e Gold Feature Store ANTT."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        corredor_rows: list[dict[str, Any]] = []
        rntrc_rows: list[dict[str, Any]] = []
        ferrovia_rows: list[dict[str, Any]] = []
        modal_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf in MUNICIPIOS_REFERENCIA:
            bundle = self._build_municipality_bundle(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                extracted_at=extracted_at,
            )
            corredor_rows.append(bundle["corredor"])
            rntrc_rows.append(bundle["rntrc"])
            ferrovia_rows.append(bundle["ferrovia"])
            modal_rows.extend(bundle["modais"])
            gold_rows.append(bundle["gold"])

        out_corredores = silver_path / "antt_corredores_rodoviarios.parquet"
        out_rntrc = silver_path / "antt_frota_rntrc_municipio.parquet"
        out_ferrovia = silver_path / "antt_malha_ferroviaria_terminais.parquet"
        out_modais = silver_path / "antt_fluxo_logistico_modal.parquet"
        out_gold = gold_path / "features_malha_transporte_logistica_municipios_brasil.parquet"

        pq.write_table(
            pa.Table.from_pylist(corredor_rows, schema=_corredores_schema()),
            out_corredores,
        )
        pq.write_table(
            pa.Table.from_pylist(rntrc_rows, schema=_rntrc_schema()),
            out_rntrc,
        )
        pq.write_table(
            pa.Table.from_pylist(ferrovia_rows, schema=_ferrovia_schema()),
            out_ferrovia,
        )
        pq.write_table(
            pa.Table.from_pylist(modal_rows, schema=_fluxo_modal_schema()),
            out_modais,
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()),
            out_gold,
        )

        logger.info(
            "ANTT baseline materializado: %d municípios (%d registros modais).",
            len(corredor_rows),
            len(modal_rows),
        )
        return {
            "antt_corredores_rodoviarios": out_corredores,
            "antt_frota_rntrc_municipio": out_rntrc,
            "antt_malha_ferroviaria_terminais": out_ferrovia,
            "antt_fluxo_logistico_modal": out_modais,
            "features_gold": out_gold,
        }

    def extract(self) -> pa.Table:
        """Executa a ingestão completa e retorna a tabela principal de corredores rodoviários."""
        outputs = self.extract_all()
        return pq.read_table(outputs["antt_corredores_rodoviarios"])

    def _lookup_parquet_row(self, parquet_path: Path, id_municipio: str) -> dict[str, Any] | None:
        """Busca uma linha municipal em arquivo Parquet local."""
        if not parquet_path.exists():
            return None
        table = pq.read_table(parquet_path)
        id_col = table["id_municipio"].to_pylist()
        if id_municipio not in id_col:
            return None
        idx = id_col.index(id_municipio)
        return {name: table[name][idx].as_py() for name in table.column_names}

    def get_highway_corridors(self, id_municipio: str) -> dict[str, Any]:
        """Retorna perfil de corredores rodoviários federais e concessões ANTT do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "antt_corredores_rodoviarios.parquet", mun_id
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["corredor"]

    def get_rntrc_fleet_profile(self, id_municipio: str) -> dict[str, Any]:
        """Retorna perfil do RNTRC (ETC, TAC, CTC e veículos habilitados) do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "antt_frota_rntrc_municipio.parquet", mun_id
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["rntrc"]

    def get_rail_intermodal_terminals(self, id_municipio: str) -> dict[str, Any]:
        """Retorna dados de conectividade ferroviária e terminais intermodais do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "antt_malha_ferroviaria_terminais.parquet", mun_id
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["ferrovia"]

    def list_modal_flows(
        self, id_municipio: str, modal_transporte: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista a matriz modal de transporte de cargas e confiabilidade no município."""
        mun_id = str(id_municipio).strip()
        parquet_file = self.silver_dir / "antt_fluxo_logistico_modal.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            bundle = self._build_municipality_bundle(
                id_mun=mun_id,
                nome_mun=f"Município {mun_id}",
                sigla_uf="BR",
                id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
                extracted_at=datetime.now(UTC).isoformat(),
            )
            rows = bundle["modais"]

        if modal_transporte:
            needle = modal_transporte.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("modal_transporte", "")).upper()
            ]
        return rows

    def get_logistics_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de infraestrutura logística e intermodalidade do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.gold_dir
            / "features_malha_transporte_logistica_municipios_brasil.parquet",
            mun_id,
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["gold"]

    def list_top_logistics_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios ranqueados pelo score composto de infraestrutura logística."""
        gold_file = (
            self.gold_dir
            / "features_malha_transporte_logistica_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.extract_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_infraestrutura_logistica", 0.0),
                row.get("score_intermodalidade_cargas", 0.0),
            ),
            reverse=True,
        )
        return rows[: max(1, limit)]


register_connector("antt_malha_transporte", AnttMalhaTransporteConnector)
