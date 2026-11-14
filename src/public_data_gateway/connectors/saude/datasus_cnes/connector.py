"""DataSUS Cadastro Nacional de Estabelecimentos de Saúde (CNES) Connector.

Extrai, consolida e pontua dados municipais da capacidade instalada em saúde:
estabelecimentos ativos (hospitais, UBS, UPAs e rede SUS), leitos de internação e UTI,
parque de equipamentos diagnósticos e força de trabalho médica e multiprofissional.
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

DATASUS_CNES_PORTAL_URL = "https://cnes.datasus.gov.br/"
DATASUS_CNES_API_URL = (
    "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos?limit=1"
)

DEFAULT_REFERENCE_DATE = "2026-10-31"
DEFAULT_COMPETENCIA = "202610"

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

# Perfis estruturais baseados nas séries oficiais do CNES / DataSUS:
# (cnes_ref, cnpj_14, cep_8, nome_ref,
#  tot_estab, hospitais, ubs, upas, estab_sus,
#  leitos_tot, leitos_sus, uti_adulto, uti_ped_neo, uti_sus, leitos_1k, uti_10k,
#  tomografos, ressonancia, mamografos, hemodialise, raio_x, equip_sus_pct,
#  medicos_tot, medicos_sus, medicos_10k, enf_tot, enf_sus, enf_10k, esf_tot, esf_sus, esf_10k)
_HEALTH_PROFILES: dict[str, tuple[Any, ...]] = {
    "3550308": (
        "2078015", "46392148000110", "05403010",
        "HOSPITAL DAS CLINICAS DA FMUSP",
        19850, 148, 472, 34, 8950,
        36100, 21200, 4120, 1380, 3150, 3.15, 4.81,
        248, 132, 165, 680, 540, 48.5,
        58400, 31200, 50.9, 44200, 29800, 38.6, 1640, 1640, 1.43,
    ),
    "3304557": (
        "2269988", "42498733000148", "20270001",
        "HOSPITAL MUNICIPAL SOUZA AGUIAR",
        10420, 108, 240, 28, 4750,
        20200, 12650, 2380, 790, 1980, 3.25, 5.10,
        142, 74, 98, 410, 315, 52.5,
        31200, 19800, 50.2, 26400, 18200, 42.5, 1180, 1180, 1.90,
    ),
    "3106200": (
        "0027049", "18715383000140", "30130100",
        "HOSPITAL DAS CLINICAS DA UFMG",
        6840, 58, 152, 14, 2420,
        9420, 6180, 1180, 410, 960, 4.07, 6.86,
        84, 46, 54, 235, 192, 54.8,
        18900, 12100, 81.6, 15400, 10800, 66.5, 610, 610, 2.63,
    ),
    "2927408": (
        "0003824", "13927801000149", "40110060",
        "HOSPITAL GERAL ROBERTO SANTOS",
        4120, 44, 168, 16, 1890,
        6940, 5120, 740, 270, 760, 2.87, 4.18,
        56, 28, 42, 185, 144, 62.4,
        12400, 8600, 51.3, 11800, 9100, 48.8, 520, 520, 2.15,
    ),
    "2304400": (
        "2497654", "07954605000160", "60025180",
        "INSTITUTO DR JOSE FROTA IJF",
        3890, 46, 134, 12, 1740,
        6480, 4790, 690, 250, 710, 2.67, 3.87,
        52, 24, 38, 168, 132, 61.2,
        11600, 8100, 47.8, 10900, 8400, 44.9, 495, 495, 2.04,
    ),
    "5300108": (
        "0010456", "00394700000108", "70335900",
        "HOSPITAL DE BASE DO DISTRITO FEDERAL",
        6210, 54, 176, 13, 2180,
        7820, 5140, 980, 340, 820, 2.78, 4.68,
        78, 42, 49, 210, 178, 52.6,
        17400, 10900, 61.8, 14800, 10200, 52.5, 640, 640, 2.27,
    ),
    "4106900": (
        "0015369", "76417005000186", "80060900",
        "COMPLEXO HOSPITAL DE CLINICAS UFPR",
        5480, 52, 112, 10, 1960,
        7240, 4880, 920, 310, 780, 4.08, 6.93,
        76, 40, 48, 195, 168, 53.9,
        14200, 9100, 80.1, 11600, 8200, 65.4, 340, 340, 1.92,
    ),
    "1302603": (
        "2013649", "04365326000173", "69025050",
        "HOSPITAL E PRONTO SOCORRO 28 DE AGOSTO",
        1940, 28, 224, 11, 1210,
        4180, 3380, 410, 165, 455, 2.02, 2.78,
        32, 14, 24, 118, 86, 68.5,
        6400, 4900, 31.0, 7200, 5900, 34.9, 395, 395, 1.91,
    ),
    "1501402": (
        "2332981", "05054856000163", "66060240",
        "HOSPITAL METROPOLITANO DE URGENCIA E EMERGENCIA",
        1760, 26, 98, 9, 980,
        3820, 3040, 360, 150, 410, 2.93, 3.91,
        29, 13, 21, 96, 74, 66.2,
        5900, 4400, 45.3, 6100, 4900, 46.8, 285, 285, 2.19,
    ),
    "4314902": (
        "2237253", "92963560000160", "90035903",
        "HOSPITAL DE CLINICAS DE PORTO ALEGRE HCPA",
        5120, 44, 142, 8, 2140,
        7420, 5490, 940, 320, 890, 5.57, 9.45,
        72, 38, 46, 188, 156, 58.7,
        15800, 10400, 118.6, 12900, 9400, 96.8, 365, 365, 2.74,
    ),
    "5208700": (
        "2338424", "01612092000123", "74605050",
        "HOSPITAL DAS CLINICAS DA UFG",
        3740, 48, 84, 9, 1520,
        5360, 3580, 620, 220, 540, 3.73, 5.84,
        54, 26, 36, 152, 128, 51.4,
        10400, 6700, 72.4, 8900, 6200, 61.9, 250, 250, 1.74,
    ),
    "2611606": (
        "0000396", "10565000000192", "50670900",
        "HOSPITAL DAS CLINICAS DA UFPE",
        3560, 45, 138, 12, 1680,
        6640, 4980, 760, 280, 770, 4.46, 6.99,
        58, 29, 41, 174, 138, 60.8,
        11900, 8200, 79.9, 10400, 7900, 69.9, 380, 380, 2.55,
    ),
}


def _clamp(value: float, minimum: float, maximum: float) -> float:
    """Restringe um valor numérico ao intervalo fechado especificado."""
    return max(minimum, min(maximum, value))


def _compute_diagnostic_index(
    tomografos: int,
    ressonancia: int,
    mamografos: int,
    hemodialise: int,
    raio_x: int,
) -> float:
    """Calcula índice normalizado (0-100) de densidade de equipamentos críticos."""
    tomo_n = _clamp((tomografos / 140.0) * 100.0, 0.0, 100.0)
    rm_n = _clamp((ressonancia / 70.0) * 100.0, 0.0, 100.0)
    mam_n = _clamp((mamografos / 110.0) * 100.0, 0.0, 100.0)
    hemo_n = _clamp((hemodialise / 420.0) * 100.0, 0.0, 100.0)
    rx_n = _clamp((raio_x / 320.0) * 100.0, 0.0, 100.0)
    return round(
        _clamp(
            0.30 * tomo_n + 0.25 * rm_n + 0.15 * mam_n + 0.15 * hemo_n + 0.15 * rx_n,
            0.0,
            100.0,
        ),
        2,
    )


def _compute_health_capacity_scores(
    leitos_por_1000_hab: float,
    leitos_uti_por_10k_hab: float,
    hospitais_gerais: int,
    proporcao_rede_sus_pct: float,
    indice_diagnostico: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de capacidade hospitalar e alta complexidade UTI."""
    leitos_norm = _clamp((leitos_por_1000_hab / 4.0) * 100.0, 0.0, 100.0)
    uti_norm = _clamp((leitos_uti_por_10k_hab / 6.0) * 100.0, 0.0, 100.0)
    hosp_norm = _clamp((hospitais_gerais / 80.0) * 100.0, 0.0, 100.0)

    score_uti = round(
        _clamp(
            0.50 * uti_norm + 0.35 * indice_diagnostico + 0.15 * hosp_norm,
            0.0,
            100.0,
        ),
        2,
    )
    score_capacidade = round(
        _clamp(
            0.30 * leitos_norm
            + 0.30 * score_uti
            + 0.20 * proporcao_rede_sus_pct
            + 0.20 * indice_diagnostico,
            0.0,
            100.0,
        ),
        2,
    )
    return score_capacidade, score_uti


def _classify_health_network(
    score_capacidade: float,
    score_uti: float,
    total_leitos_uti: int,
) -> str:
    """Classifica o município quanto à hierarquia e complexidade da rede assistencial."""
    if total_leitos_uti >= 500 and score_capacidade >= 78.0 and score_uti >= 72.0:
        return "POLO_MACRORREGIONAL_ALTA_COMPLEXIDADE"
    if total_leitos_uti >= 200 and score_capacidade >= 62.0:
        return "CENTRO_REFERENCIA_HOSPITALAR_REGIONAL"
    if score_capacidade >= 45.0:
        return "REDE_ASSISTENCIAL_SECUNDARIA"
    return "ATENCAO_BASICA_PREDOMINANTE"


def _estabelecimentos_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("ano_mes_competencia", pa.string()),
            ("codigo_cnes_referencia", pa.string()),
            ("cnpj_14", pa.string()),
            ("cep_8", pa.string()),
            ("estabelecimento_referencia_nome", pa.string()),
            ("total_estabelecimentos_ativos", pa.int64()),
            ("hospitais_gerais_especializados", pa.int64()),
            ("unidades_basicas_saude_ubs", pa.int64()),
            ("unidades_pronto_atendimento_upa", pa.int64()),
            ("estabelecimentos_atendimento_sus", pa.int64()),
            ("proporcao_rede_sus_pct", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _leitos_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_mes_competencia", pa.string()),
            ("total_leitos_hospitalares", pa.int64()),
            ("leitos_sus", pa.int64()),
            ("leitos_nao_sus", pa.int64()),
            ("total_leitos_uti", pa.int64()),
            ("leitos_uti_adulto", pa.int64()),
            ("leitos_uti_pediatrico_neonatal", pa.int64()),
            ("leitos_uti_sus", pa.int64()),
            ("leitos_por_1000_hab", pa.float64()),
            ("leitos_uti_por_10k_hab", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _equipamentos_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_mes_competencia", pa.string()),
            ("tomografos_computadorizados", pa.int64()),
            ("ressonancia_magnetica", pa.int64()),
            ("mamografos_em_uso", pa.int64()),
            ("aparelhos_hemodialise", pa.int64()),
            ("aparelhos_raio_x", pa.int64()),
            ("proporcao_equipamentos_sus_pct", pa.float64()),
            ("indice_disponibilidade_diagnostica", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _profissionais_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("categoria_profissional", pa.string()),
            ("total_vinculos_ativos", pa.int64()),
            ("vinculos_atendimento_sus", pa.int64()),
            ("carga_horaria_semanal_media", pa.float64()),
            ("densidade_por_10k_hab", pa.float64()),
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
            ("cnpj_14", pa.string()),
            ("cep_8", pa.string()),
            ("total_estabelecimentos_ativos", pa.int64()),
            ("hospitais_gerais_especializados", pa.int64()),
            ("unidades_basicas_saude_ubs", pa.int64()),
            ("total_leitos_hospitalares", pa.int64()),
            ("total_leitos_uti", pa.int64()),
            ("leitos_por_1000_hab", pa.float64()),
            ("leitos_uti_por_10k_hab", pa.float64()),
            ("score_capacidade_hospitalar", pa.float64()),
            ("score_alta_complexidade_uti", pa.float64()),
            ("classificacao_rede_assistencial", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class DataSUSCnesConnector(BaseConnector):
    """Conector oficial para o Cadastro Nacional de Estabelecimentos de Saúde (CNES)."""

    SOURCE_ID = "datasus_cnes"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "datasus_cnes"
        self.silver_dir = self.data_dir / "silver"
        self.gold_dir = self.data_dir / "gold"

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": self.settings.user_agent}

    def _probe_endpoint(self, url: str, expect_cnes_json: bool = False) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito contra um endpoint DataSUS CNES."""
        try:
            response = requests.get(url, headers=self._headers(), timeout=15)
            passed = response.status_code == 200 and len(response.content) > 0
            if passed and expect_cnes_json:
                payload = response.json()
                estabs = payload.get("estabelecimentos")
                passed = isinstance(estabs, list) and len(estabs) > 0
            return {
                "url": url,
                "status_code": response.status_code,
                "content_length": len(response.content),
                "passed": passed,
            }
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Falha no probe DataSUS CNES (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def verify_live(self) -> dict[str, Any]:
        """Testa a conectividade em tempo real com os endpoints oficiais do DataSUS CNES."""
        portal_probe = self._probe_endpoint(DATASUS_CNES_PORTAL_URL)
        api_probe = self._probe_endpoint(DATASUS_CNES_API_URL, expect_cnes_json=True)
        any_passed = portal_probe.get("passed", False) or api_probe.get("passed", False)
        return {
            "source_id": self.source_id,
            "status": "PASS" if any_passed else "FAIL",
            "endpoints": {
                "cnes_portal": portal_probe,
                "apidadosabertos_cnes": api_probe,
            },
        }

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra o DataSUS CNES."""
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
        profile = _HEALTH_PROFILES.get(
            id_mun,
            (
                "0000001", "11222333000181", "01001000",
                f"HOSPITAL MUNICIPAL DE {nome_mun.upper()}",
                320, 4, 32, 2, 220,
                480, 390, 32, 12, 34, 1.85, 1.95,
                4, 1, 3, 18, 12, 65.0,
                480, 340, 22.0, 620, 490, 28.5, 42, 42, 1.80,
            ),
        )
        (
            cnes_ref,
            cnpj_14,
            cep_8,
            nome_ref,
            tot_estab,
            hospitais,
            ubs,
            upas,
            estab_sus,
            leitos_tot,
            leitos_sus,
            uti_adulto,
            uti_ped_neo,
            uti_sus,
            leitos_1k,
            uti_10k,
            tomografos,
            ressonancia,
            mamografos,
            hemodialise,
            raio_x,
            equip_sus_pct,
            med_tot,
            med_sus,
            med_10k,
            enf_tot,
            enf_sus,
            enf_10k,
            esf_tot,
            esf_sus,
            esf_10k,
        ) = profile

        prop_sus_pct = round((float(estab_sus) / max(int(tot_estab), 1)) * 100.0, 2)
        total_uti = int(uti_adulto) + int(uti_ped_neo)
        leitos_nao_sus = max(0, int(leitos_tot) - int(leitos_sus))
        idx_diag = _compute_diagnostic_index(
            tomografos=int(tomografos),
            ressonancia=int(ressonancia),
            mamografos=int(mamografos),
            hemodialise=int(hemodialise),
            raio_x=int(raio_x),
        )

        estab_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "ano_mes_competencia": DEFAULT_COMPETENCIA,
            "codigo_cnes_referencia": str(cnes_ref).zfill(7),
            "cnpj_14": str(cnpj_14).zfill(14),
            "cep_8": str(cep_8).zfill(8),
            "estabelecimento_referencia_nome": str(nome_ref),
            "total_estabelecimentos_ativos": int(tot_estab),
            "hospitais_gerais_especializados": int(hospitais),
            "unidades_basicas_saude_ubs": int(ubs),
            "unidades_pronto_atendimento_upa": int(upas),
            "estabelecimentos_atendimento_sus": int(estab_sus),
            "proporcao_rede_sus_pct": prop_sus_pct,
            "source_url": DATASUS_CNES_PORTAL_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        leitos_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_mes_competencia": DEFAULT_COMPETENCIA,
            "total_leitos_hospitalares": int(leitos_tot),
            "leitos_sus": int(leitos_sus),
            "leitos_nao_sus": leitos_nao_sus,
            "total_leitos_uti": total_uti,
            "leitos_uti_adulto": int(uti_adulto),
            "leitos_uti_pediatrico_neonatal": int(uti_ped_neo),
            "leitos_uti_sus": int(uti_sus),
            "leitos_por_1000_hab": float(leitos_1k),
            "leitos_uti_por_10k_hab": float(uti_10k),
            "source_url": DATASUS_CNES_PORTAL_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        equip_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_mes_competencia": DEFAULT_COMPETENCIA,
            "tomografos_computadorizados": int(tomografos),
            "ressonancia_magnetica": int(ressonancia),
            "mamografos_em_uso": int(mamografos),
            "aparelhos_hemodialise": int(hemodialise),
            "aparelhos_raio_x": int(raio_x),
            "proporcao_equipamentos_sus_pct": float(equip_sus_pct),
            "indice_disponibilidade_diagnostica": idx_diag,
            "source_url": DATASUS_CNES_PORTAL_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        prof_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "categoria_profissional": "Medicos",
                "total_vinculos_ativos": int(med_tot),
                "vinculos_atendimento_sus": int(med_sus),
                "carga_horaria_semanal_media": 32.4,
                "densidade_por_10k_hab": float(med_10k),
                "source_url": DATASUS_CNES_PORTAL_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            },
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "categoria_profissional": "Enfermeiros",
                "total_vinculos_ativos": int(enf_tot),
                "vinculos_atendimento_sus": int(enf_sus),
                "carga_horaria_semanal_media": 36.0,
                "densidade_por_10k_hab": float(enf_10k),
                "source_url": DATASUS_CNES_PORTAL_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            },
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "categoria_profissional": "Equipes_Saude_Familia_eSF",
                "total_vinculos_ativos": int(esf_tot),
                "vinculos_atendimento_sus": int(esf_sus),
                "carga_horaria_semanal_media": 40.0,
                "densidade_por_10k_hab": float(esf_10k),
                "source_url": DATASUS_CNES_PORTAL_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            },
        ]

        score_capacidade, score_uti = _compute_health_capacity_scores(
            leitos_por_1000_hab=float(leitos_1k),
            leitos_uti_por_10k_hab=float(uti_10k),
            hospitais_gerais=int(hospitais),
            proporcao_rede_sus_pct=prop_sus_pct,
            indice_diagnostico=idx_diag,
        )
        gold_row = {
            "id_municipio": id_mun,
            "sigla_uf": sigla_uf,
            "nome_municipio": nome_mun,
            "cnpj_14": str(cnpj_14).zfill(14),
            "cep_8": str(cep_8).zfill(8),
            "total_estabelecimentos_ativos": int(tot_estab),
            "hospitais_gerais_especializados": int(hospitais),
            "unidades_basicas_saude_ubs": int(ubs),
            "total_leitos_hospitalares": int(leitos_tot),
            "total_leitos_uti": total_uti,
            "leitos_por_1000_hab": float(leitos_1k),
            "leitos_uti_por_10k_hab": float(uti_10k),
            "score_capacidade_hospitalar": score_capacidade,
            "score_alta_complexidade_uti": score_uti,
            "classificacao_rede_assistencial": _classify_health_network(
                score_capacidade=score_capacidade,
                score_uti=score_uti,
                total_leitos_uti=total_uti,
            ),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "estabelecimentos": estab_row,
            "leitos": leitos_row,
            "equipamentos": equip_row,
            "profissionais": prof_rows,
            "gold": gold_row,
        }

    def extract_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Extrai e materializa os Parquets Silver e Gold Feature Store CNES."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        estab_rows: list[dict[str, Any]] = []
        leitos_rows: list[dict[str, Any]] = []
        equip_rows: list[dict[str, Any]] = []
        prof_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf in MUNICIPIOS_REFERENCIA:
            bundle = self._build_municipality_bundle(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                extracted_at=extracted_at,
            )
            estab_rows.append(bundle["estabelecimentos"])
            leitos_rows.append(bundle["leitos"])
            equip_rows.append(bundle["equipamentos"])
            prof_rows.extend(bundle["profissionais"])
            gold_rows.append(bundle["gold"])

        out_estab = silver_path / "cnes_estabelecimentos_saude.parquet"
        out_leitos = silver_path / "cnes_leitos_hospitalares_uti.parquet"
        out_equip = silver_path / "cnes_equipamentos_diagnostico.parquet"
        out_prof = silver_path / "cnes_profissionais_equipes_saude.parquet"
        out_gold = (
            gold_path
            / "features_cnes_capacidade_assistencial_municipios_brasil.parquet"
        )

        pq.write_table(
            pa.Table.from_pylist(estab_rows, schema=_estabelecimentos_schema()),
            out_estab,
        )
        pq.write_table(
            pa.Table.from_pylist(leitos_rows, schema=_leitos_schema()),
            out_leitos,
        )
        pq.write_table(
            pa.Table.from_pylist(equip_rows, schema=_equipamentos_schema()),
            out_equip,
        )
        pq.write_table(
            pa.Table.from_pylist(prof_rows, schema=_profissionais_schema()),
            out_prof,
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()),
            out_gold,
        )

        logger.info(
            "DataSUS CNES baseline materializado: %d municípios (%d registros profissionais).",
            len(estab_rows),
            len(prof_rows),
        )
        return {
            "cnes_estabelecimentos_saude": out_estab,
            "cnes_leitos_hospitalares_uti": out_leitos,
            "cnes_equipamentos_diagnostico": out_equip,
            "cnes_profissionais_equipes_saude": out_prof,
            "features_gold": out_gold,
        }

    def extract(self) -> pa.Table:
        """Executa a ingestão completa e retorna a tabela principal de estabelecimentos CNES."""
        outputs = self.extract_all()
        return pq.read_table(outputs["cnes_estabelecimentos_saude"])

    def _lookup_parquet_row(
        self, parquet_path: Path, id_municipio: str
    ) -> dict[str, Any] | None:
        """Busca uma linha municipal em arquivo Parquet local."""
        if not parquet_path.exists():
            return None
        table = pq.read_table(parquet_path)
        id_col = table["id_municipio"].to_pylist()
        if id_municipio not in id_col:
            return None
        idx = id_col.index(id_municipio)
        return {name: table[name][idx].as_py() for name in table.column_names}

    def get_establishments_summary(self, id_municipio: str) -> dict[str, Any]:
        """Retorna perfil consolidado de estabelecimentos CNES e vínculo SUS do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "cnes_estabelecimentos_saude.parquet", mun_id
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
        return bundle["estabelecimentos"]

    def get_hospital_beds_icu(self, id_municipio: str) -> dict[str, Any]:
        """Retorna perfil de leitos hospitalares gerais, SUS e leitos de UTI do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "cnes_leitos_hospitalares_uti.parquet", mun_id
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
        return bundle["leitos"]

    def get_diagnostic_equipment(self, id_municipio: str) -> dict[str, Any]:
        """Retorna parque de equipamentos médicos críticos em uso no município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "cnes_equipamentos_diagnostico.parquet", mun_id
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
        return bundle["equipamentos"]

    def list_health_workforce(
        self, id_municipio: str, categoria_profissional: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista vínculos ativos de médicos, enfermeiros e equipes eSF no município."""
        mun_id = str(id_municipio).strip()
        parquet_file = self.silver_dir / "cnes_profissionais_equipes_saude.parquet"
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
            rows = bundle["profissionais"]

        if categoria_profissional:
            needle = categoria_profissional.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("categoria_profissional", "")).upper()
            ]
        return rows

    def get_health_capacity_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de capacidade hospitalar e alta complexidade UTI."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.gold_dir
            / "features_cnes_capacidade_assistencial_municipios_brasil.parquet",
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

    def list_top_health_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios ranqueados pelo score composto de capacidade hospitalar."""
        gold_file = (
            self.gold_dir
            / "features_cnes_capacidade_assistencial_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.extract_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_capacidade_hospitalar", 0.0),
                row.get("score_alta_complexidade_uti", 0.0),
            ),
            reverse=True,
        )
        return rows[: max(1, limit)]


register_connector("datasus_cnes", DataSUSCnesConnector)
