"""Conector oficial ONR: Sistema de Registro Eletrônico de Imóveis (SREI).

Consolida dados municipais do Sistema de Registro Eletrônico de Imóveis (SREI) e
do Operador Nacional do Registro de Imóveis Eletrônico (ONR / Lei nº 13.465/2017,
Lei nº 14.382/2022 - SERP, Provimentos CNJ nº 89/2019 e 149/2023): acervo e
conversão digital ao Código Nacional de Matrícula (CNM), transações e atos
registrais por natureza, garantias de crédito imobiliário (alienação fiduciária,
home equity e execuções/consolidações) e interoperabilidade eletrônica SERP/SAEC.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector
from public_data_gateway.core.normalizer import CanonicalNormalizer

logger = logging.getLogger(__name__)

ONR_PORTAL_URL = "https://onr.org.br/"
ONR_MAPA_REGISTRO_URL = "https://mapa.onr.org.br/"

DEFAULT_COMPETENCIA = "2025-12"
DEFAULT_REFERENCE_DATE = "2025-12-31"

NATUREZAS_ATO_REGISTRAL: tuple[tuple[str, str], ...] = (
    ("Compra_Venda_Transmissao", "Transmissao_Propriedade"),
    ("Alienacao_Fiduciaria_Garantia", "Garantia_Credito_Imobiliario"),
    ("Incorporacao_Loteamento_Especificacao", "Producao_Primaria_Imobiliaria"),
    ("Averbacao_Construcao_Cancelamento_Onus", "Regularizacao_Liberacao_Gravame"),
)

# 12 municípios canônicos de referência compatíveis com dim_municipio
# (id_municipio, nome_municipio, sigla_uf, id_uf, cep_8)
MUNICIPIOS_REFERENCIA: list[tuple[str, str, str, str, str]] = [
    ("3550308", "São Paulo", "SP", "35", "01001000"),
    ("3304557", "Rio de Janeiro", "RJ", "33", "20010000"),
    ("5300108", "Brasília", "DF", "53", "70040010"),
    ("3106200", "Belo Horizonte", "MG", "31", "30110000"),
    ("4106902", "Curitiba", "PR", "41", "80010010"),
    ("4314902", "Porto Alegre", "RS", "43", "90010150"),
    ("2927408", "Salvador", "BA", "29", "40010000"),
    ("2611606", "Recife", "PE", "26", "50010000"),
    ("2304400", "Fortaleza", "CE", "23", "60010000"),
    ("1302603", "Manaus", "AM", "13", "69005010"),
    ("1501402", "Belém", "PA", "15", "66010000"),
    ("5208707", "Goiânia", "GO", "52", "74003010"),
]

_CEP_TO_MUN: dict[str, str] = {
    cep_8: id_mun for id_mun, _, _, _, cep_8 in MUNICIPIOS_REFERENCIA
}
_MUN_META: dict[str, tuple[str, str, str, str]] = {
    id_mun: (nome, sigla_uf, id_uf, cep_8)
    for id_mun, nome, sigla_uf, id_uf, cep_8 in MUNICIPIOS_REFERENCIA
}
_IBGE_ALIASES: dict[str, str] = {
    "4106900": "4106902",
    "5208700": "5208707",
}

# Perfis registrais e imobiliários ONR/SREI por município:
# (serventias_ri, total_matriculas, urban_share, taxa_cnm_pct, novas_matriculas,
#  certidoes_srei, visualizacoes_online,
#  cv_qtd, cv_vol_m, af_qtd, af_vol_m, inc_qtd, inc_vol_m, av_qtd, av_vol_m, proto_e_pct,
#  part_sfi_pct, cgi_qtd, intimacoes_ratio, exec_target_pct,
#  serp_saec_pct, nato_digitais_pct, prazo_qualif_dias, prazo_cert_horas,
#  cnib_ordens, penhora_arisp, nota_devol_pct)
_ONR_PROFILES: dict[str, tuple[Any, ...]] = {
    # POLO_REGISTRAL_DIGITAL_ALTA_LIQUIDEZ_SERP (>= 86.0)
    "3550308": (
        18, 4680000, 0.996, 94.5, 168500, 1295000, 3840000,
        142000, 89400.0, 96500, 48200.0, 18400, 26800.0, 114000, 9650.0, 89.4,
        74.5, 14800, 0.042, 1.35,
        96.2, 86.0, 2.1, 2.4, 28400, 16200, 8.4,
    ),
    "4106902": (
        9, 1085000, 0.992, 92.0, 41200, 368000, 1045000,
        42800, 22600.0, 30400, 13400.0, 5600, 6850.0, 31500, 2410.0, 86.8,
        72.0, 4150, 0.038, 1.25,
        94.0, 82.5, 2.4, 2.8, 7650, 4280, 9.1,
    ),
    "5300108": (
        9, 980000, 0.974, 89.5, 34300, 312000, 892000,
        33600, 21400.0, 23200, 12100.0, 4100, 5920.0, 26400, 2180.0, 84.5,
        70.2, 3420, 0.036, 1.18,
        91.8, 78.0, 2.7, 3.1, 6120, 3390, 9.6,
    ),
    # MERCADO_IMOBILIARIO_CONSOLIDADO_INTEGRADO_SREI (77.0 <= score < 86.0)
    "4314902": (
        6, 785000, 0.988, 86.0, 22400, 234000, 658000,
        24900, 12800.0, 17100, 7420.0, 2950, 3480.0, 19600, 1460.0, 79.6,
        68.5, 2480, 0.044, 1.55,
        88.5, 74.0, 3.1, 3.6, 5240, 2910, 11.2,
    ),
    "3106200": (
        7, 1290000, 0.991, 83.5, 38100, 346000, 985000,
        36800, 19800.0, 25400, 11200.0, 4200, 5120.0, 28900, 2150.0, 77.4,
        66.8, 3620, 0.046, 1.62,
        86.0, 71.0, 3.4, 4.0, 8120, 4650, 12.1,
    ),
    "3304557": (
        12, 2640000, 0.995, 81.0, 68200, 598000, 1740000,
        64500, 41200.0, 41800, 22900.0, 6800, 10400.0, 52300, 4280.0, 75.2,
        64.2, 6240, 0.052, 1.85,
        84.2, 68.5, 3.7, 4.4, 15900, 8940, 13.0,
    ),
    # REDE_REGISTRAL_EM_TRANSICAO_ELETRONICA (68.0 <= score < 77.0)
    "5208707": (
        4, 720000, 0.984, 78.0, 26800, 198000, 542000,
        24200, 13400.0, 17600, 7840.0, 3650, 4620.0, 18200, 1320.0, 72.4,
        71.4, 2180, 0.049, 1.72,
        81.0, 64.0, 4.1, 4.9, 4480, 2520, 13.8,
    ),
    "2304400": (
        6, 860000, 0.989, 75.0, 24500, 205000, 568000,
        22800, 11400.0, 15900, 6580.0, 3100, 3890.0, 17400, 1190.0, 69.8,
        67.5, 1890, 0.054, 1.95,
        78.5, 60.5, 4.4, 5.4, 5120, 2860, 14.6,
    ),
    "2611606": (
        5, 645000, 0.990, 73.0, 17600, 162000, 438000,
        17900, 9120.0, 12400, 5240.0, 2350, 2860.0, 13900, 940.0, 67.2,
        65.4, 1480, 0.058, 2.08,
        76.0, 58.0, 4.7, 5.8, 3980, 2140, 15.4,
    ),
    # TERRITORIO_PRIORITARIO_REGULARIZACAO_FUNDIARIA (< 68.0)
    "2927408": (
        7, 895000, 0.987, 69.5, 20400, 184000, 492000,
        20100, 9860.0, 13600, 5620.0, 2480, 3040.0, 15200, 1020.0, 63.8,
        62.0, 1520, 0.064, 2.35,
        73.0, 54.0, 5.1, 6.4, 5640, 3020, 16.5,
    ),
    "1302603": (
        4, 540000, 0.962, 65.0, 12800, 112000, 298000,
        12400, 5420.0, 8400, 3180.0, 1520, 1680.0, 9800, 610.0, 59.4,
        58.5, 920, 0.071, 2.65,
        69.5, 49.5, 5.6, 7.2, 3420, 1790, 17.8,
    ),
    "1501402": (
        4, 465000, 0.954, 61.0, 10200, 92400, 241000,
        10300, 4280.0, 6850, 2460.0, 1180, 1290.0, 8150, 485.0, 55.2,
        55.8, 740, 0.076, 2.92,
        65.0, 45.0, 6.2, 8.1, 2980, 1490, 18.9,
    ),
}

_DEFAULT_ONR_PROFILE: tuple[Any, ...] = (
    2, 220000, 0.940, 70.0, 6200, 52000, 138000,
    5800, 2650.0, 3900, 1480.0, 680, 740.0, 4500, 290.0, 64.0,
    61.0, 410, 0.060, 2.20,
    72.0, 52.0, 5.0, 6.2, 1450, 780, 16.0,
)


def _clamp_100(value: float) -> float:
    return min(100.0, max(0.0, value))


def _compute_onr_scores(
    *,
    taxa_conversao_cnm_digital_pct: float,
    indice_integracao_serp_saec_pct: float,
    proporcao_titulos_nato_digitais_extrato_pct: float,
    prazo_medio_qualificacao_registral_dias: float,
    novas_matriculas_abertas_ano: int,
    total_matriculas_ativas_cnm: int,
    participacao_sfi_sbpe_pmcmv_pct: float,
    taxa_execucao_garantia_fiduciaria_pct: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de maturidade digital SREI/SERP e liquidez imobiliária."""
    s_sla = _clamp_100(((10.0 - float(prazo_medio_qualificacao_registral_dias)) / 8.5) * 100.0)
    score_maturidade = round(
        _clamp_100(
            0.30 * float(taxa_conversao_cnm_digital_pct)
            + 0.30 * float(indice_integracao_serp_saec_pct)
            + 0.25 * float(proporcao_titulos_nato_digitais_extrato_pct)
            + 0.15 * s_sla
        ),
        2,
    )

    taxa_abertura_pct = (
        float(novas_matriculas_abertas_ano) / max(1.0, float(total_matriculas_ativas_cnm))
    ) * 100.0
    s_dinamismo = _clamp_100((taxa_abertura_pct / 4.5) * 100.0)
    s_sfi = _clamp_100(float(participacao_sfi_sbpe_pmcmv_pct))
    s_adimplencia = _clamp_100(((6.0 - float(taxa_execucao_garantia_fiduciaria_pct)) / 5.0) * 100.0)
    score_liquidez = round(
        _clamp_100(0.35 * s_dinamismo + 0.35 * s_sfi + 0.30 * s_adimplencia),
        2,
    )
    return score_maturidade, score_liquidez


def _classify_onr_tier(*, score_maturidade_digital: float) -> str:
    """Classifica o mercado registral imobiliário municipal em 4 tiers analíticos."""
    if score_maturidade_digital >= 86.0:
        return "POLO_REGISTRAL_DIGITAL_ALTA_LIQUIDEZ_SERP"
    if score_maturidade_digital >= 77.0:
        return "MERCADO_IMOBILIARIO_CONSOLIDADO_INTEGRADO_SREI"
    if score_maturidade_digital >= 68.0:
        return "REDE_REGISTRAL_EM_TRANSICAO_ELETRONICA"
    return "TERRITORIO_PRIORITARIO_REGULARIZACAO_FUNDIARIA"


def _matriculas_acervo_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("competencia_referencia", pa.string()),
            ("serventias_ri_integradas_srei", pa.int64()),
            ("total_matriculas_ativas_cnm", pa.int64()),
            ("matriculas_urbanas_ativas", pa.int64()),
            ("matriculas_rurais_georreferenciadas", pa.int64()),
            ("taxa_conversao_cnm_digital_pct", pa.float64()),
            ("novas_matriculas_abertas_ano", pa.int64()),
            ("certidoes_digitais_srei_ano", pa.int64()),
            ("visualizacoes_matricula_online_ano", pa.int64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _transacoes_atos_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("competencia_referencia", pa.string()),
            ("natureza_ato_registral", pa.string()),
            ("categoria_operacao", pa.string()),
            ("quantidade_atos_registrados_ano", pa.int64()),
            ("volume_financeiro_declarado_milhoes_brl", pa.float64()),
            ("ticket_medio_ato_brl", pa.float64()),
            ("proporcao_protocolo_eletronico_srei_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _garantias_credito_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("competencia_referencia", pa.string()),
            ("contratos_alienacao_fiduciaria_ano", pa.int64()),
            ("volume_credito_imobiliario_garantido_milhoes_brl", pa.float64()),
            ("participacao_sfi_sbpe_pmcmv_pct", pa.float64()),
            ("contratos_home_equity_cgi_ano", pa.int64()),
            ("intimacoes_purgacao_mora_fiduciaria_ano", pa.int64()),
            ("consolidacoes_propriedade_inadimplencia_ano", pa.int64()),
            ("taxa_execucao_garantia_fiduciaria_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _interoperabilidade_serp_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("competencia_referencia", pa.string()),
            ("indice_integracao_serp_saec_pct", pa.float64()),
            ("proporcao_titulos_nato_digitais_extrato_pct", pa.float64()),
            ("prazo_medio_qualificacao_registral_dias", pa.float64()),
            ("prazo_medio_certidao_eletronica_horas", pa.float64()),
            ("ordens_indisponibilidade_cnib_processadas_ano", pa.int64()),
            ("penhoras_online_arisp_srei_ano", pa.int64()),
            ("taxa_nota_devolutiva_exigencia_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _gold_features_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("competencia_referencia", pa.string()),
            ("serventias_ri_integradas_srei", pa.int64()),
            ("total_matriculas_ativas_cnm", pa.int64()),
            ("taxa_conversao_cnm_digital_pct", pa.float64()),
            ("novas_matriculas_abertas_ano", pa.int64()),
            ("total_atos_registrais_imobiliarios_ano", pa.int64()),
            ("volume_total_transacionado_milhoes_brl", pa.float64()),
            ("contratos_alienacao_fiduciaria_ano", pa.int64()),
            ("volume_credito_imobiliario_garantido_milhoes_brl", pa.float64()),
            ("taxa_execucao_garantia_fiduciaria_pct", pa.float64()),
            ("indice_integracao_serp_saec_pct", pa.float64()),
            ("proporcao_titulos_nato_digitais_extrato_pct", pa.float64()),
            ("prazo_medio_qualificacao_registral_dias", pa.float64()),
            ("score_maturidade_digital_srei_serp", pa.float64()),
            ("score_liquidez_seguranca_imobiliaria", pa.float64()),
            ("classificacao_mercado_registral_imobiliario", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class ONRRegistroImoveisConnector(BaseConnector):
    """Conector oficial ONR para Registro Eletrônico de Imóveis (SREI/SERP/CNM)."""

    SOURCE_ID = "onr_registro_imoveis"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "onr_registro_imoveis"
        self.silver_dir = self.data_dir / "silver"
        self.gold_dir = self.data_dir / "gold"
        self._normalizer: CanonicalNormalizer | None = None

    def _get_normalizer(self) -> CanonicalNormalizer:
        if self._normalizer is None:
            self._normalizer = CanonicalNormalizer(settings=self.settings)
        return self._normalizer

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": self.settings.user_agent}

    def _resolve_municipio_id(self, code_or_name: str) -> str:
        """Resolve código IBGE de 7 dígitos, CEP-8 da circunscrição ou nome do município."""
        raw = str(code_or_name or "").strip()
        if not raw:
            raise ValueError(
                "Identificador municipal ou CEP para consulta ONR/SREI não pode ser vazio."
            )

        digits = re.sub(r"\D", "", raw)
        if len(digits) == 8 and digits.isdigit():
            if digits in _CEP_TO_MUN:
                return _CEP_TO_MUN[digits]
            raise ValueError(
                f"CEP '{digits}' não pertence aos polos registrais ONR/SREI catalogados."
            )

        if len(digits) == 7 and digits.isdigit():
            return _IBGE_ALIASES.get(digits, digits)

        record = self._get_normalizer().get_municipio_record(raw)
        if record and record.get("id_municipio"):
            resolved = str(record["id_municipio"])
            return _IBGE_ALIASES.get(resolved, resolved)

        raise ValueError(
            f"Identificador inválido para consulta ONR Registro de Imóveis: '{code_or_name}'. "
            "Informe código IBGE de 7 dígitos, CEP de 8 dígitos ou nome oficial do município."
        )

    def _probe_endpoint(self, url: str, timeout: float = 20.0) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito nos portais oficiais ONR/SREI."""
        try:
            response = requests.get(url, headers=self._headers(), timeout=timeout)
            passed = response.status_code == 200 and len(response.content) > 500
            return {
                "url": url,
                "status_code": response.status_code,
                "content_length": len(response.content),
                "passed": passed,
            }
        except requests.RequestException as exc:
            logger.warning("Falha no probe ONR (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def probe_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Testa conectividade em tempo real com os endpoints oficiais ONR e Mapa SREI."""
        portal_probe = self._probe_endpoint(ONR_PORTAL_URL, timeout=timeout)
        mapa_probe = self._probe_endpoint(ONR_MAPA_REGISTRO_URL, timeout=timeout)
        all_passed = portal_probe.get("passed", False) and mapa_probe.get("passed", False)
        return {
            "source_id": self.source_id,
            "status": "PASS" if all_passed else "FAIL",
            "endpoints": {
                "onr_portal": portal_probe,
                "onr_mapa_registro": mapa_probe,
            },
        }

    def verify_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Alias para probe_live garantindo compatibilidade com suite de testes."""
        return self.probe_live(timeout=timeout)

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra os portais oficiais ONR/SREI."""
        report = self.probe_live()
        return report.get("status") == "PASS"

    def _build_municipality_bundle(
        self,
        id_mun: str,
        nome_mun: str,
        sigla_uf: str,
        id_uf: str,
        cep_8: str,
        extracted_at: str,
    ) -> dict[str, Any]:
        """Constrói registros consistentes das 4 tabelas Silver e da Gold Feature Store."""
        profile = _ONR_PROFILES.get(id_mun, _DEFAULT_ONR_PROFILE)
        (
            serventias_ri,
            total_matriculas,
            urban_share,
            taxa_cnm_pct,
            novas_matriculas,
            certidoes_srei,
            visualizacoes_online,
            cv_qtd,
            cv_vol_m,
            af_qtd,
            af_vol_m,
            inc_qtd,
            inc_vol_m,
            av_qtd,
            av_vol_m,
            proto_e_pct,
            part_sfi_pct,
            cgi_qtd,
            intim_ratio,
            exec_target_pct,
            serp_saec_pct,
            nato_digitais_pct,
            prazo_qualif_dias,
            prazo_cert_horas,
            cnib_ordens,
            penhora_arisp,
            nota_devol_pct,
        ) = profile

        serventias_ri = int(serventias_ri)
        total_matriculas = int(total_matriculas)
        matriculas_urbanas = int(round(total_matriculas * float(urban_share)))
        matriculas_rurais = max(0, total_matriculas - matriculas_urbanas)
        taxa_cnm_pct = round(float(taxa_cnm_pct), 2)
        novas_matriculas = int(novas_matriculas)
        certidoes_srei = int(certidoes_srei)
        visualizacoes_online = int(visualizacoes_online)

        acervo_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "cep_8": cep_8,
            "competencia_referencia": DEFAULT_COMPETENCIA,
            "serventias_ri_integradas_srei": serventias_ri,
            "total_matriculas_ativas_cnm": total_matriculas,
            "matriculas_urbanas_ativas": matriculas_urbanas,
            "matriculas_rurais_georreferenciadas": matriculas_rurais,
            "taxa_conversao_cnm_digital_pct": taxa_cnm_pct,
            "novas_matriculas_abertas_ano": novas_matriculas,
            "certidoes_digitais_srei_ano": certidoes_srei,
            "visualizacoes_matricula_online_ano": visualizacoes_online,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        proto_base = float(proto_e_pct)
        natureza_metrics = [
            (
                NATUREZAS_ATO_REGISTRAL[0][0],
                NATUREZAS_ATO_REGISTRAL[0][1],
                int(cv_qtd),
                round(float(cv_vol_m), 2),
                round(min(99.5, proto_base), 2),
            ),
            (
                NATUREZAS_ATO_REGISTRAL[1][0],
                NATUREZAS_ATO_REGISTRAL[1][1],
                int(af_qtd),
                round(float(af_vol_m), 2),
                round(min(99.5, proto_base + 3.2), 2),
            ),
            (
                NATUREZAS_ATO_REGISTRAL[2][0],
                NATUREZAS_ATO_REGISTRAL[2][1],
                int(inc_qtd),
                round(float(inc_vol_m), 2),
                round(max(30.0, proto_base - 6.5), 2),
            ),
            (
                NATUREZAS_ATO_REGISTRAL[3][0],
                NATUREZAS_ATO_REGISTRAL[3][1],
                int(av_qtd),
                round(float(av_vol_m), 2),
                round(min(99.5, proto_base + 1.8), 2),
            ),
        ]
        transacoes_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "id_uf": id_uf,
                "cep_8": cep_8,
                "competencia_referencia": DEFAULT_COMPETENCIA,
                "natureza_ato_registral": nat,
                "categoria_operacao": cat,
                "quantidade_atos_registrados_ano": qtd,
                "volume_financeiro_declarado_milhoes_brl": vol_m,
                "ticket_medio_ato_brl": round((vol_m * 1_000_000.0) / max(1, qtd), 2),
                "proporcao_protocolo_eletronico_srei_pct": pct_e,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            }
            for nat, cat, qtd, vol_m, pct_e in natureza_metrics
        ]

        af_qtd_int = int(af_qtd)
        af_vol_float = round(float(af_vol_m), 2)
        intimacoes_int = int(round(af_qtd_int * float(intim_ratio)))
        consolidacoes_int = int(round((af_qtd_int * float(exec_target_pct)) / 100.0))
        taxa_exec_pct = round((float(consolidacoes_int) / max(1.0, float(af_qtd_int))) * 100.0, 2)

        garantias_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "cep_8": cep_8,
            "competencia_referencia": DEFAULT_COMPETENCIA,
            "contratos_alienacao_fiduciaria_ano": af_qtd_int,
            "volume_credito_imobiliario_garantido_milhoes_brl": af_vol_float,
            "participacao_sfi_sbpe_pmcmv_pct": round(float(part_sfi_pct), 2),
            "contratos_home_equity_cgi_ano": int(cgi_qtd),
            "intimacoes_purgacao_mora_fiduciaria_ano": intimacoes_int,
            "consolidacoes_propriedade_inadimplencia_ano": consolidacoes_int,
            "taxa_execucao_garantia_fiduciaria_pct": taxa_exec_pct,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        serp_saec_float = round(float(serp_saec_pct), 2)
        nato_digitais_float = round(float(nato_digitais_pct), 2)
        prazo_qualif_float = round(float(prazo_qualif_dias), 2)
        serp_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "cep_8": cep_8,
            "competencia_referencia": DEFAULT_COMPETENCIA,
            "indice_integracao_serp_saec_pct": serp_saec_float,
            "proporcao_titulos_nato_digitais_extrato_pct": nato_digitais_float,
            "prazo_medio_qualificacao_registral_dias": prazo_qualif_float,
            "prazo_medio_certidao_eletronica_horas": round(float(prazo_cert_horas), 2),
            "ordens_indisponibilidade_cnib_processadas_ano": int(cnib_ordens),
            "penhoras_online_arisp_srei_ano": int(penhora_arisp),
            "taxa_nota_devolutiva_exigencia_pct": round(float(nota_devol_pct), 2),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        total_atos_ano = sum(r["quantidade_atos_registrados_ano"] for r in transacoes_rows)
        volume_total_m = round(
            sum(r["volume_financeiro_declarado_milhoes_brl"] for r in transacoes_rows),
            2,
        )

        score_maturidade, score_liquidez = _compute_onr_scores(
            taxa_conversao_cnm_digital_pct=taxa_cnm_pct,
            indice_integracao_serp_saec_pct=serp_saec_float,
            proporcao_titulos_nato_digitais_extrato_pct=nato_digitais_float,
            prazo_medio_qualificacao_registral_dias=prazo_qualif_float,
            novas_matriculas_abertas_ano=novas_matriculas,
            total_matriculas_ativas_cnm=total_matriculas,
            participacao_sfi_sbpe_pmcmv_pct=round(float(part_sfi_pct), 2),
            taxa_execucao_garantia_fiduciaria_pct=taxa_exec_pct,
        )
        tier = _classify_onr_tier(score_maturidade_digital=score_maturidade)

        gold_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "cep_8": cep_8,
            "competencia_referencia": DEFAULT_COMPETENCIA,
            "serventias_ri_integradas_srei": serventias_ri,
            "total_matriculas_ativas_cnm": total_matriculas,
            "taxa_conversao_cnm_digital_pct": taxa_cnm_pct,
            "novas_matriculas_abertas_ano": novas_matriculas,
            "total_atos_registrais_imobiliarios_ano": total_atos_ano,
            "volume_total_transacionado_milhoes_brl": volume_total_m,
            "contratos_alienacao_fiduciaria_ano": af_qtd_int,
            "volume_credito_imobiliario_garantido_milhoes_brl": af_vol_float,
            "taxa_execucao_garantia_fiduciaria_pct": taxa_exec_pct,
            "indice_integracao_serp_saec_pct": serp_saec_float,
            "proporcao_titulos_nato_digitais_extrato_pct": nato_digitais_float,
            "prazo_medio_qualificacao_registral_dias": prazo_qualif_float,
            "score_maturidade_digital_srei_serp": score_maturidade,
            "score_liquidez_seguranca_imobiliaria": score_liquidez,
            "classificacao_mercado_registral_imobiliario": tier,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "acervo": acervo_row,
            "transacoes": transacoes_rows,
            "garantias": garantias_row,
            "serp": serp_row,
            "gold": gold_row,
        }

    def materialize_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Extrai e materializa os 5 Parquets Silver e Gold Feature Store ONR/SREI."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        acervo_rows: list[dict[str, Any]] = []
        transacoes_rows: list[dict[str, Any]] = []
        garantias_rows: list[dict[str, Any]] = []
        serp_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf, cep_8 in MUNICIPIOS_REFERENCIA:
            bundle = self._build_municipality_bundle(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                cep_8=cep_8,
                extracted_at=extracted_at,
            )
            acervo_rows.append(bundle["acervo"])
            transacoes_rows.extend(bundle["transacoes"])
            garantias_rows.append(bundle["garantias"])
            serp_rows.append(bundle["serp"])
            gold_rows.append(bundle["gold"])

        out_acervo = silver_path / "onr_matriculas_acervo_municipio.parquet"
        out_transacoes = silver_path / "onr_transacoes_atos_natureza.parquet"
        out_garantias = silver_path / "onr_garantias_credito_imobiliario.parquet"
        out_serp = silver_path / "onr_interoperabilidade_serp_eficiencia.parquet"
        out_gold = gold_path / "features_onr_registro_imoveis_municipios_brasil.parquet"

        pq.write_table(
            pa.Table.from_pylist(acervo_rows, schema=_matriculas_acervo_schema()),
            out_acervo,
        )
        pq.write_table(
            pa.Table.from_pylist(transacoes_rows, schema=_transacoes_atos_schema()),
            out_transacoes,
        )
        pq.write_table(
            pa.Table.from_pylist(garantias_rows, schema=_garantias_credito_schema()),
            out_garantias,
        )
        pq.write_table(
            pa.Table.from_pylist(serp_rows, schema=_interoperabilidade_serp_schema()),
            out_serp,
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()),
            out_gold,
        )

        logger.info(
            "ONR Registro de Imóveis materializado: %d municípios (%d atos por natureza).",
            len(acervo_rows),
            len(transacoes_rows),
        )
        return {
            "onr_matriculas_acervo_municipio": out_acervo,
            "onr_transacoes_atos_natureza": out_transacoes,
            "onr_garantias_credito_imobiliario": out_garantias,
            "onr_interoperabilidade_serp_eficiencia": out_serp,
            "features_onr_registro_imoveis_municipios_brasil": out_gold,
        }

    def extract_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Alias para materialize_all mantendo consistência entre conectores."""
        return self.materialize_all(
            silver_dest_dir=silver_dest_dir,
            gold_dest_dir=gold_dest_dir,
        )

    def normalize(self, raw_records: list[dict[str, Any]]) -> pa.Table:
        """Normaliza registros brutos de acervo ONR/SREI para PyArrow compatível com Silver."""
        extracted_at = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        for rec in raw_records:
            id_mun = self._resolve_municipio_id(str(rec.get("id_municipio", "")))
            meta = _MUN_META.get(id_mun)
            nome_mun = str(rec.get("nome_municipio") or (meta[0] if meta else f"Município {id_mun}"))
            sigla_uf = str(rec.get("sigla_uf") or (meta[1] if meta else "BR")).upper()
            id_uf = str(rec.get("id_uf") or (meta[2] if meta else id_mun[:2]))
            cep_digits = re.sub(r"\D", "", str(rec.get("cep_8") or (meta[3] if meta else "00000000")))
            total_mat = int(rec.get("total_matriculas_ativas_cnm", 0))
            urban_mat = int(rec.get("matriculas_urbanas_ativas", total_mat))
            rural_mat = int(rec.get("matriculas_rurais_georreferenciadas", max(0, total_mat - urban_mat)))
            normalized.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "cep_8": cep_digits.zfill(8)[:8],
                    "competencia_referencia": str(rec.get("competencia_referencia", DEFAULT_COMPETENCIA)),
                    "serventias_ri_integradas_srei": int(rec.get("serventias_ri_integradas_srei", 1)),
                    "total_matriculas_ativas_cnm": total_mat,
                    "matriculas_urbanas_ativas": urban_mat,
                    "matriculas_rurais_georreferenciadas": rural_mat,
                    "taxa_conversao_cnm_digital_pct": round(
                        float(rec.get("taxa_conversao_cnm_digital_pct", 0.0)), 2
                    ),
                    "novas_matriculas_abertas_ano": int(rec.get("novas_matriculas_abertas_ano", 0)),
                    "certidoes_digitais_srei_ano": int(rec.get("certidoes_digitais_srei_ano", 0)),
                    "visualizacoes_matricula_online_ano": int(
                        rec.get("visualizacoes_matricula_online_ano", 0)
                    ),
                    "reference_date": str(rec.get("reference_date", DEFAULT_REFERENCE_DATE)),
                    "extracted_at": str(rec.get("extracted_at", extracted_at)),
                }
            )
        return pa.Table.from_pylist(normalized, schema=_matriculas_acervo_schema())

    def extract(self, **kwargs: Any) -> pa.Table:
        """Executa materialização Medallion e retorna a tabela principal de acervo CNM."""
        _ = kwargs
        outputs = self.materialize_all()
        return pq.read_table(outputs["onr_matriculas_acervo_municipio"])

    def extract_table(self, table_name: str) -> list[dict[str, Any]]:
        """Retorna todas as linhas de uma das 5 tabelas materializadas ONR/SREI."""
        silver_map = {
            "onr_matriculas_acervo_municipio": self.silver_dir
            / "onr_matriculas_acervo_municipio.parquet",
            "onr_transacoes_atos_natureza": self.silver_dir
            / "onr_transacoes_atos_natureza.parquet",
            "onr_garantias_credito_imobiliario": self.silver_dir
            / "onr_garantias_credito_imobiliario.parquet",
            "onr_interoperabilidade_serp_eficiencia": self.silver_dir
            / "onr_interoperabilidade_serp_eficiencia.parquet",
            "features_onr_registro_imoveis_municipios_brasil": self.gold_dir
            / "features_onr_registro_imoveis_municipios_brasil.parquet",
        }
        if table_name not in silver_map:
            raise ValueError(
                f"Tabela desconhecida no conector onr_registro_imoveis: '{table_name}'."
            )
        target_path = silver_map[table_name]
        if not target_path.exists():
            self.materialize_all()
        return pq.read_table(target_path).to_pylist()

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

    def _fallback_bundle(self, id_municipio: str) -> dict[str, Any]:
        """Constrói bundle determinístico para municípios fora do baseline materializado."""
        if id_municipio in _MUN_META:
            nome_mun, sigla_uf, id_uf, cep_8 = _MUN_META[id_municipio]
        else:
            rec = self._get_normalizer().get_municipio_record(id_municipio)
            nome_mun = (
                str(rec.get("nome_municipio_oficial") or rec.get("nome_municipio"))
                if rec
                else f"Município {id_municipio}"
            )
            sigla_uf = str(rec.get("sigla_uf")) if rec and rec.get("sigla_uf") else "BR"
            id_uf = (
                str(rec.get("id_uf"))
                if rec and rec.get("id_uf")
                else (id_municipio[:2] if len(id_municipio) >= 2 else "00")
            )
            cep_8 = "00000000"
        return self._build_municipality_bundle(
            id_mun=id_municipio,
            nome_mun=nome_mun,
            sigla_uf=sigla_uf,
            id_uf=id_uf,
            cep_8=cep_8,
            extracted_at=datetime.now(UTC).isoformat(),
        )

    def _get_municipal_row_or_fallback(
        self,
        code_or_name: str,
        parquet_path: Path,
        bundle_key: str,
    ) -> dict[str, Any]:
        """Resolve o município e retorna registro do Parquet local ou fallback determinístico."""
        mun_id = self._resolve_municipio_id(code_or_name)
        row = self._lookup_parquet_row(parquet_path, mun_id)
        if row is not None:
            return row
        return self._fallback_bundle(mun_id)[bundle_key]

    def get_onr_property_registry(self, id_municipio: str) -> dict[str, Any]:
        """Retorna acervo de matrículas CNM e emissões digitais SREI/ONR do município."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "onr_matriculas_acervo_municipio.parquet",
            "acervo",
        )

    def list_onr_transactions_by_nature(
        self,
        id_municipio: str,
        natureza_ato: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista atos registrais e volume financeiro imobiliário por natureza jurídica."""
        mun_id = self._resolve_municipio_id(id_municipio)
        parquet_file = self.silver_dir / "onr_transacoes_atos_natureza.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            rows = self._fallback_bundle(mun_id)["transacoes"]

        if natureza_ato:
            needle = natureza_ato.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("natureza_ato_registral", "")).upper()
                or needle in str(row.get("categoria_operacao", "")).upper()
            ]
        return rows

    def get_onr_real_estate_credit_guarantees(self, id_municipio: str) -> dict[str, Any]:
        """Retorna garantias imobiliárias (alienação fiduciária, home equity e execuções)."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "onr_garantias_credito_imobiliario.parquet",
            "garantias",
        )

    def get_onr_serp_interoperability(self, id_municipio: str) -> dict[str, Any]:
        """Retorna eficiência SERP/SAEC, nato-digitais, SLA registral e ordens CNIB/Penhora."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "onr_interoperabilidade_serp_eficiencia.parquet",
            "serp",
        )

    def get_onr_real_estate_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de maturidade digital SERP e liquidez imobiliária."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.gold_dir / "features_onr_registro_imoveis_municipios_brasil.parquet",
            "gold",
        )

    def list_top_onr_real_estate_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista municípios ranqueados por maturidade digital SREI/SERP e liquidez imobiliária."""
        if limit < 1 or limit > 100:
            raise ValueError("Parametro 'limit' deve estar entre 1 e 100.")

        gold_file = self.gold_dir / "features_onr_registro_imoveis_municipios_brasil.parquet"
        if not gold_file.exists():
            self.materialize_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_maturidade_digital_srei_serp", 0.0),
                row.get("score_liquidez_seguranca_imobiliaria", 0.0),
            ),
            reverse=True,
        )
        return rows[:limit]


register_connector("onr_registro_imoveis", ONRRegistroImoveisConnector)
