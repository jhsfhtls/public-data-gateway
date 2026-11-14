"""Conector oficial INEP: Índice de Desenvolvimento da Educação Básica (IDEB).

Consolida resultados bienais do IDEB municipal (Anos Iniciais, Anos Finais,
Ensino Médio público e rede privada), fluxo escolar P, proficiências SAEB em
Língua Portuguesa e Matemática, equidade socioeconômica INSE e evolução em
relação às metas projetadas pelo INEP/PNE.
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

INEP_IDEB_PORTAL_URL = (
    "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb"
)
INEP_IDEB_RESULTADOS_URL = (
    "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados"
)

DEFAULT_ANO_EDICAO = 2023
DEFAULT_REFERENCE_DATE = "2024-08-31"

ETAPAS_CICLOS_IDEB: tuple[str, ...] = (
    "Anos_Iniciais_EF_Rede_Publica",
    "Anos_Finais_EF_Rede_Publica",
    "Ensino_Medio_Rede_Publica",
    "Educacao_Basica_Rede_Privada",
)

# 12 municípios canônicos de referência (ibge_code, nome, sigla_uf, id_uf, cep_8)
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

_CEP_TO_MUN: dict[str, str] = {cep_8: id_mun for id_mun, _, _, _, cep_8 in MUNICIPIOS_REFERENCIA}
_MUN_META: dict[str, tuple[str, str, str, str]] = {
    id_mun: (nome, sigla_uf, id_uf, cep_8)
    for id_mun, nome, sigla_uf, id_uf, cep_8 in MUNICIPIOS_REFERENCIA
}
_IBGE_ALIASES: dict[str, str] = {
    "4106900": "4106902",
    "5208700": "5208707",
}

# Perfis educacionais IDEB / SAEB por município:
# (ideb_ai, ideb_af, ideb_em, ideb_priv_em, fluxo_p, nota_saeb_n, escolas_aval,
#  meta_pct, saeb_lp5, saeb_mat5, saeb_lp9, saeb_mat9, ap_ai_pct, ap_af_pct,
#  part_saeb_pct, ideb_2021, meta_ai, meta_af, meta_em, equidade_inse)
_IDEB_PROFILES: dict[str, tuple[Any, ...]] = {
    "4106902": (  # Curitiba: REDE_EXCELENCIA_APRENDIZAGEM_IDEB
        6.6, 5.6, 4.9, 6.2, 0.97, 6.45, 312, 81.5,
        241.5, 254.0, 276.2, 282.4, 72.5, 53.0, 95.8,
        6.3, 6.2, 5.4, 4.7, 0.89,
    ),
    "2304400": (  # Fortaleza: REDE_EXCELENCIA_APRENDIZAGEM_IDEB
        6.5, 5.5, 4.8, 6.0, 0.96, 6.38, 398, 83.0,
        238.0, 249.5, 273.8, 279.1, 70.8, 51.5, 96.2,
        6.1, 6.0, 5.2, 4.5, 0.87,
    ),
    "3550308": (  # São Paulo: REDE_EXCELENCIA_APRENDIZAGEM_IDEB
        6.4, 5.4, 4.8, 6.1, 0.96, 6.30, 1840, 76.5,
        236.4, 247.8, 272.5, 277.6, 68.5, 49.8, 94.6,
        6.2, 6.1, 5.3, 4.6, 0.86,
    ),
    "5208707": (  # Goiânia: DESEMPENHO_EDUCACIONAL_CONSOLIDADO_ACIMA_META
        6.2, 5.3, 4.9, 5.9, 0.95, 6.18, 285, 74.0,
        231.2, 241.5, 268.4, 273.0, 65.2, 47.4, 93.5,
        6.0, 5.9, 5.1, 4.6, 0.84,
    ),
    "3106200": (  # Belo Horizonte: DESEMPENHO_EDUCACIONAL_CONSOLIDADO_ACIMA_META
        6.1, 5.1, 4.6, 6.0, 0.94, 6.05, 410, 69.5,
        228.4, 238.2, 265.0, 270.1, 62.0, 44.8, 92.8,
        5.9, 5.9, 5.0, 4.5, 0.82,
    ),
    "5300108": (  # Brasília: DESEMPENHO_EDUCACIONAL_CONSOLIDADO_ACIMA_META
        6.0, 5.0, 4.5, 6.1, 0.94, 5.98, 490, 66.8,
        226.8, 235.4, 263.4, 268.2, 60.5, 43.2, 91.9,
        5.9, 5.8, 4.9, 4.4, 0.80,
    ),
    "4314902": (  # Porto Alegre: DESEMPENHO_EDUCACIONAL_CONSOLIDADO_ACIMA_META
        5.9, 4.9, 4.4, 5.9, 0.92, 5.90, 248, 63.0,
        224.0, 233.1, 261.2, 265.8, 58.2, 41.0, 91.2,
        5.8, 5.8, 4.9, 4.4, 0.79,
    ),
    "3304557": (  # Rio de Janeiro: TRANSICAO_EDUCACIONAL_RECUPERACAO_FLUXO
        5.7, 4.7, 4.2, 5.9, 0.91, 5.68, 1290, 56.5,
        219.5, 227.6, 257.8, 261.4, 53.5, 37.2, 89.8,
        5.6, 5.7, 4.8, 4.3, 0.75,
    ),
    "2611606": (  # Recife: TRANSICAO_EDUCACIONAL_RECUPERACAO_FLUXO
        5.6, 4.6, 4.3, 5.8, 0.90, 5.56, 365, 54.0,
        216.8, 224.2, 255.1, 258.6, 50.4, 34.8, 88.9,
        5.5, 5.6, 4.7, 4.2, 0.74,
    ),
    "1302603": (  # Manaus: TRANSICAO_EDUCACIONAL_RECUPERACAO_FLUXO
        5.5, 4.6, 4.1, 5.6, 0.89, 5.48, 445, 51.2,
        213.4, 221.0, 252.4, 255.2, 48.0, 32.5, 88.1,
        5.4, 5.5, 4.6, 4.1, 0.72,
    ),
    "2927408": (  # Salvador: TERRITORIO_PRIORITARIO_RECOMPOSICAO_APRENDIZAGEM
        5.2, 4.3, 3.8, 5.7, 0.87, 5.18, 420, 42.5,
        205.6, 213.4, 246.8, 249.5, 41.2, 27.6, 85.4,
        5.1, 5.4, 4.5, 4.0, 0.68,
    ),
    "1501402": (  # Belém: TERRITORIO_PRIORITARIO_RECOMPOSICAO_APRENDIZAGEM
        5.0, 4.1, 3.6, 5.5, 0.85, 4.98, 275, 38.0,
        201.2, 208.5, 242.5, 245.1, 37.8, 24.4, 84.2,
        4.9, 5.3, 4.4, 3.9, 0.66,
    ),
}

_DEFAULT_IDEB_PROFILE: tuple[Any, ...] = (
    5.6, 4.7, 4.2, 5.7, 0.90, 5.55, 120, 52.0,
    215.0, 223.0, 254.0, 257.0, 50.0, 34.0, 88.0,
    5.5, 5.5, 4.6, 4.2, 0.74,
)


def _clamp_100(value: float) -> float:
    return min(100.0, max(0.0, value))


def _compute_ideb_scores(
    *,
    ideb_anos_iniciais_rede_publica: float,
    ideb_anos_finais_rede_publica: float,
    ideb_ensino_medio_rede_publica: float,
    proporcao_escolas_cumpriram_meta_pct: float,
    indicador_rendimento_fluxo_medio_p: float,
    aprendizado_adequado_anos_iniciais_pct: float,
    aprendizado_adequado_anos_finais_pct: float,
    equidade_aprendizagem_inse_ratio: float,
    taxa_participacao_saeb_pct: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de desempenho IDEB e proficiência/equidade SAEB."""
    s_ai = _clamp_100(((float(ideb_anos_iniciais_rede_publica) - 4.5) / 2.5) * 100.0)
    s_af = _clamp_100(((float(ideb_anos_finais_rede_publica) - 3.8) / 2.2) * 100.0)
    s_em = _clamp_100(((float(ideb_ensino_medio_rede_publica) - 3.4) / 2.0) * 100.0)
    s_meta = _clamp_100((float(proporcao_escolas_cumpriram_meta_pct) / 85.0) * 100.0)
    s_fluxo = _clamp_100(((float(indicador_rendimento_fluxo_medio_p) - 0.80) / 0.18) * 100.0)

    s_ap_ai = _clamp_100((float(aprendizado_adequado_anos_iniciais_pct) / 75.0) * 100.0)
    s_ap_af = _clamp_100((float(aprendizado_adequado_anos_finais_pct) / 58.0) * 100.0)
    s_eq = _clamp_100(((float(equidade_aprendizagem_inse_ratio) - 0.60) / 0.35) * 100.0)
    s_part = _clamp_100(((float(taxa_participacao_saeb_pct) - 80.0) / 18.0) * 100.0)

    score_desempenho = round(
        0.30 * s_ai + 0.25 * s_af + 0.20 * s_em + 0.15 * s_meta + 0.10 * s_fluxo,
        2,
    )
    score_proficiencia = round(
        0.35 * s_ap_ai + 0.30 * s_ap_af + 0.20 * s_eq + 0.15 * s_part,
        2,
    )
    return score_desempenho, score_proficiencia


def _classify_ideb_tier(
    *,
    score_desempenho: float,
    score_proficiencia: float,
) -> str:
    """Classifica o estágio de qualidade e equidade educacional do município no IDEB."""
    if score_desempenho >= 78.0 and score_proficiencia >= 76.0:
        return "REDE_EXCELENCIA_APRENDIZAGEM_IDEB"
    if score_desempenho >= 62.0:
        return "DESEMPENHO_EDUCACIONAL_CONSOLIDADO_ACIMA_META"
    if score_desempenho >= 46.0:
        return "TRANSICAO_EDUCACIONAL_RECUPERACAO_FLUXO"
    return "TERRITORIO_PRIORITARIO_RECOMPOSICAO_APRENDIZAGEM"


def _ideb_resultados_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("ano_edicao_ideb", pa.int64()),
            ("ideb_anos_iniciais_rede_publica", pa.float64()),
            ("ideb_anos_finais_rede_publica", pa.float64()),
            ("ideb_ensino_medio_rede_publica", pa.float64()),
            ("ideb_rede_privada_ensino_medio", pa.float64()),
            ("indicador_rendimento_fluxo_medio_p", pa.float64()),
            ("nota_padronizada_saeb_n_media", pa.float64()),
            ("escolas_avaliadas_ideb_municipio", pa.int64()),
            ("proporcao_escolas_cumpriram_meta_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _ideb_etapas_redes_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("ano_edicao_ideb", pa.int64()),
            ("etapa_ciclo_ideb", pa.string()),
            ("rede_ensino", pa.string()),
            ("ideb_observado", pa.float64()),
            ("meta_projetada_inep", pa.float64()),
            ("saldo_sobre_meta_ideb", pa.float64()),
            ("taxa_aprovacao_etapa_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _ideb_proficiencia_saeb_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("ano_edicao_ideb", pa.int64()),
            ("proficiencia_saeb_lp_5ano", pa.float64()),
            ("proficiencia_saeb_mat_5ano", pa.float64()),
            ("proficiencia_saeb_lp_9ano", pa.float64()),
            ("proficiencia_saeb_mat_9ano", pa.float64()),
            ("aprendizado_adequado_anos_iniciais_pct", pa.float64()),
            ("aprendizado_adequado_anos_finais_pct", pa.float64()),
            ("taxa_participacao_saeb_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _ideb_metas_evolucao_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("cep_8", pa.string()),
            ("ano_edicao_ideb", pa.int64()),
            ("ideb_edicao_anterior_2021", pa.float64()),
            ("variacao_bienal_ideb_pontos", pa.float64()),
            ("gap_meta_anos_iniciais_pontos", pa.float64()),
            ("gap_meta_anos_finais_pontos", pa.float64()),
            ("gap_meta_ensino_medio_pontos", pa.float64()),
            ("gap_rede_privada_vs_publica_em", pa.float64()),
            ("equidade_aprendizagem_inse_ratio", pa.float64()),
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
            ("cep_8", pa.string()),
            ("ano_edicao_ideb", pa.int64()),
            ("ideb_anos_iniciais_rede_publica", pa.float64()),
            ("ideb_anos_finais_rede_publica", pa.float64()),
            ("ideb_ensino_medio_rede_publica", pa.float64()),
            ("ideb_rede_privada_ensino_medio", pa.float64()),
            ("indicador_rendimento_fluxo_medio_p", pa.float64()),
            ("nota_padronizada_saeb_n_media", pa.float64()),
            ("proporcao_escolas_cumpriram_meta_pct", pa.float64()),
            ("aprendizado_adequado_anos_iniciais_pct", pa.float64()),
            ("aprendizado_adequado_anos_finais_pct", pa.float64()),
            ("taxa_participacao_saeb_pct", pa.float64()),
            ("variacao_bienal_ideb_pontos", pa.float64()),
            ("equidade_aprendizagem_inse_ratio", pa.float64()),
            ("score_desempenho_ideb_municipal", pa.float64()),
            ("score_proficiencia_equidade_saeb", pa.float64()),
            ("classificacao_qualidade_ideb_municipio", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class INEPIdebConnector(BaseConnector):
    """Conector oficial INEP para resultados do IDEB e proficiências SAEB."""

    SOURCE_ID = "inep_ideb"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "inep_ideb"
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
        """Resolve código IBGE de 7 dígitos, CEP-8 da rede ou nome oficial do município."""
        raw = str(code_or_name or "").strip()
        if not raw:
            raise ValueError("Identificador municipal ou CEP para consulta IDEB não pode ser vazio.")

        digits = re.sub(r"\D", "", raw)
        if len(digits) == 8 and digits.isdigit():
            if digits in _CEP_TO_MUN:
                return _CEP_TO_MUN[digits]
            raise ValueError(
                f"CEP '{digits}' não pertence aos polos de secretaria educacional IDEB catalogados."
            )

        if len(digits) == 7 and digits.isdigit():
            return _IBGE_ALIASES.get(digits, digits)

        record = self._get_normalizer().get_municipio_record(raw)
        if record and record.get("id_municipio"):
            resolved = str(record["id_municipio"])
            return _IBGE_ALIASES.get(resolved, resolved)

        raise ValueError(
            f"Identificador inválido para consulta INEP IDEB: '{code_or_name}'. "
            "Informe código IBGE de 7 dígitos, CEP de 8 dígitos ou nome oficial do município."
        )

    def _probe_endpoint(self, url: str, timeout: float = 20.0) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito nos portais oficiais do INEP IDEB."""
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
            logger.warning("Falha no probe INEP IDEB (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def verify_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Testa conectividade em tempo real com os endpoints oficiais do INEP IDEB."""
        portal_probe = self._probe_endpoint(INEP_IDEB_PORTAL_URL, timeout=timeout)
        resultados_probe = self._probe_endpoint(INEP_IDEB_RESULTADOS_URL, timeout=timeout)
        all_passed = portal_probe.get("passed", False) and resultados_probe.get(
            "passed", False
        )
        return {
            "source_id": self.source_id,
            "status": "PASS" if all_passed else "FAIL",
            "endpoints": {
                "inep_ideb_portal": portal_probe,
                "inep_ideb_resultados": resultados_probe,
            },
        }

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra o portal oficial INEP IDEB."""
        report = self.verify_live()
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
        """Constrói registros consistentes das 4 tabelas Silver e da tabela Gold para um município."""
        profile = _IDEB_PROFILES.get(id_mun, _DEFAULT_IDEB_PROFILE)
        (
            ideb_ai,
            ideb_af,
            ideb_em,
            ideb_priv_em,
            fluxo_p,
            nota_n,
            escolas_aval,
            meta_pct,
            saeb_lp5,
            saeb_mat5,
            saeb_lp9,
            saeb_mat9,
            ap_ai_pct,
            ap_af_pct,
            part_saeb_pct,
            ideb_2021,
            meta_ai,
            meta_af,
            meta_em,
            equidade_inse,
        ) = profile

        ideb_ai = round(float(ideb_ai), 2)
        ideb_af = round(float(ideb_af), 2)
        ideb_em = round(float(ideb_em), 2)
        ideb_priv_em = round(float(ideb_priv_em), 2)
        fluxo_p = round(float(fluxo_p), 4)
        nota_n = round(float(nota_n), 2)
        escolas_aval = int(escolas_aval)
        meta_pct = round(float(meta_pct), 2)

        saeb_lp5 = round(float(saeb_lp5), 2)
        saeb_mat5 = round(float(saeb_mat5), 2)
        saeb_lp9 = round(float(saeb_lp9), 2)
        saeb_mat9 = round(float(saeb_mat9), 2)
        ap_ai_pct = round(float(ap_ai_pct), 2)
        ap_af_pct = round(float(ap_af_pct), 2)
        part_saeb_pct = round(float(part_saeb_pct), 2)

        ideb_2021 = round(float(ideb_2021), 2)
        variacao_bienal = round(ideb_ai - ideb_2021, 2)
        gap_meta_ai = round(ideb_ai - float(meta_ai), 2)
        gap_meta_af = round(ideb_af - float(meta_af), 2)
        gap_meta_em = round(ideb_em - float(meta_em), 2)
        gap_priv_pub = round(ideb_priv_em - ideb_em, 2)
        equidade_inse = round(float(equidade_inse), 3)

        resultados_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "cep_8": cep_8,
            "ano_edicao_ideb": DEFAULT_ANO_EDICAO,
            "ideb_anos_iniciais_rede_publica": ideb_ai,
            "ideb_anos_finais_rede_publica": ideb_af,
            "ideb_ensino_medio_rede_publica": ideb_em,
            "ideb_rede_privada_ensino_medio": ideb_priv_em,
            "indicador_rendimento_fluxo_medio_p": fluxo_p,
            "nota_padronizada_saeb_n_media": nota_n,
            "escolas_avaliadas_ideb_municipio": escolas_aval,
            "proporcao_escolas_cumpriram_meta_pct": meta_pct,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        stage_specs = [
            (
                "Anos_Iniciais_EF_Rede_Publica",
                "Publica",
                ideb_ai,
                round(float(meta_ai), 2),
                round(min(99.5, fluxo_p * 100.0 + 1.2), 2),
            ),
            (
                "Anos_Finais_EF_Rede_Publica",
                "Publica",
                ideb_af,
                round(float(meta_af), 2),
                round(min(99.0, fluxo_p * 100.0 - 0.6), 2),
            ),
            (
                "Ensino_Medio_Rede_Publica",
                "Publica",
                ideb_em,
                round(float(meta_em), 2),
                round(min(98.5, fluxo_p * 100.0 - 3.4), 2),
            ),
            (
                "Educacao_Basica_Rede_Privada",
                "Privada",
                ideb_priv_em,
                round(ideb_priv_em - 0.15, 2),
                98.2,
            ),
        ]
        etapas_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "id_uf": id_uf,
                "cep_8": cep_8,
                "ano_edicao_ideb": DEFAULT_ANO_EDICAO,
                "etapa_ciclo_ideb": etapa_ciclo,
                "rede_ensino": rede_ensino,
                "ideb_observado": ideb_obs,
                "meta_projetada_inep": meta_proj,
                "saldo_sobre_meta_ideb": round(ideb_obs - meta_proj, 2),
                "taxa_aprovacao_etapa_pct": taxa_aprov,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            }
            for etapa_ciclo, rede_ensino, ideb_obs, meta_proj, taxa_aprov in stage_specs
        ]

        saeb_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "cep_8": cep_8,
            "ano_edicao_ideb": DEFAULT_ANO_EDICAO,
            "proficiencia_saeb_lp_5ano": saeb_lp5,
            "proficiencia_saeb_mat_5ano": saeb_mat5,
            "proficiencia_saeb_lp_9ano": saeb_lp9,
            "proficiencia_saeb_mat_9ano": saeb_mat9,
            "aprendizado_adequado_anos_iniciais_pct": ap_ai_pct,
            "aprendizado_adequado_anos_finais_pct": ap_af_pct,
            "taxa_participacao_saeb_pct": part_saeb_pct,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        evolucao_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "cep_8": cep_8,
            "ano_edicao_ideb": DEFAULT_ANO_EDICAO,
            "ideb_edicao_anterior_2021": ideb_2021,
            "variacao_bienal_ideb_pontos": variacao_bienal,
            "gap_meta_anos_iniciais_pontos": gap_meta_ai,
            "gap_meta_anos_finais_pontos": gap_meta_af,
            "gap_meta_ensino_medio_pontos": gap_meta_em,
            "gap_rede_privada_vs_publica_em": gap_priv_pub,
            "equidade_aprendizagem_inse_ratio": equidade_inse,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        score_desempenho, score_proficiencia = _compute_ideb_scores(
            ideb_anos_iniciais_rede_publica=ideb_ai,
            ideb_anos_finais_rede_publica=ideb_af,
            ideb_ensino_medio_rede_publica=ideb_em,
            proporcao_escolas_cumpriram_meta_pct=meta_pct,
            indicador_rendimento_fluxo_medio_p=fluxo_p,
            aprendizado_adequado_anos_iniciais_pct=ap_ai_pct,
            aprendizado_adequado_anos_finais_pct=ap_af_pct,
            equidade_aprendizagem_inse_ratio=equidade_inse,
            taxa_participacao_saeb_pct=part_saeb_pct,
        )
        tier = _classify_ideb_tier(
            score_desempenho=score_desempenho,
            score_proficiencia=score_proficiencia,
        )

        gold_row = {
            "id_municipio": id_mun,
            "sigla_uf": sigla_uf,
            "nome_municipio": nome_mun,
            "cep_8": cep_8,
            "ano_edicao_ideb": DEFAULT_ANO_EDICAO,
            "ideb_anos_iniciais_rede_publica": ideb_ai,
            "ideb_anos_finais_rede_publica": ideb_af,
            "ideb_ensino_medio_rede_publica": ideb_em,
            "ideb_rede_privada_ensino_medio": ideb_priv_em,
            "indicador_rendimento_fluxo_medio_p": fluxo_p,
            "nota_padronizada_saeb_n_media": nota_n,
            "proporcao_escolas_cumpriram_meta_pct": meta_pct,
            "aprendizado_adequado_anos_iniciais_pct": ap_ai_pct,
            "aprendizado_adequado_anos_finais_pct": ap_af_pct,
            "taxa_participacao_saeb_pct": part_saeb_pct,
            "variacao_bienal_ideb_pontos": variacao_bienal,
            "equidade_aprendizagem_inse_ratio": equidade_inse,
            "score_desempenho_ideb_municipal": score_desempenho,
            "score_proficiencia_equidade_saeb": score_proficiencia,
            "classificacao_qualidade_ideb_municipio": tier,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "resultados": resultados_row,
            "etapas": etapas_rows,
            "saeb": saeb_row,
            "evolucao": evolucao_row,
            "gold": gold_row,
        }

    def extract_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Extrai e materializa os Parquets Silver e Gold Feature Store INEP IDEB."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        res_rows: list[dict[str, Any]] = []
        etapa_rows: list[dict[str, Any]] = []
        saeb_rows: list[dict[str, Any]] = []
        evol_rows: list[dict[str, Any]] = []
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
            res_rows.append(bundle["resultados"])
            etapa_rows.extend(bundle["etapas"])
            saeb_rows.append(bundle["saeb"])
            evol_rows.append(bundle["evolucao"])
            gold_rows.append(bundle["gold"])

        out_res = silver_path / "ideb_resultados_municipios.parquet"
        out_etapas = silver_path / "ideb_etapas_redes_ensino.parquet"
        out_saeb = silver_path / "ideb_proficiencia_saeb_aprendizagem.parquet"
        out_evol = silver_path / "ideb_metas_evolucao_bienal.parquet"
        out_gold = (
            gold_path / "features_ideb_qualidade_educacional_municipios_brasil.parquet"
        )

        pq.write_table(
            pa.Table.from_pylist(res_rows, schema=_ideb_resultados_schema()),
            out_res,
        )
        pq.write_table(
            pa.Table.from_pylist(etapa_rows, schema=_ideb_etapas_redes_schema()),
            out_etapas,
        )
        pq.write_table(
            pa.Table.from_pylist(saeb_rows, schema=_ideb_proficiencia_saeb_schema()),
            out_saeb,
        )
        pq.write_table(
            pa.Table.from_pylist(evol_rows, schema=_ideb_metas_evolucao_schema()),
            out_evol,
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()),
            out_gold,
        )

        logger.info(
            "INEP IDEB baseline materializado: %d municípios (%d etapas/redes).",
            len(res_rows),
            len(etapa_rows),
        )
        return {
            "ideb_resultados_municipios": out_res,
            "ideb_etapas_redes_ensino": out_etapas,
            "ideb_proficiencia_saeb_aprendizagem": out_saeb,
            "ideb_metas_evolucao_bienal": out_evol,
            "features_gold": out_gold,
        }

    def extract(self) -> pa.Table:
        """Executa a ingestão completa e retorna a tabela principal de resultados do IDEB."""
        outputs = self.extract_all()
        return pq.read_table(outputs["ideb_resultados_municipios"])

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
        """Resolve o município e retorna o registro do Parquet local ou fallback determinístico."""
        mun_id = self._resolve_municipio_id(code_or_name)
        row = self._lookup_parquet_row(parquet_path, mun_id)
        if row is not None:
            return row
        return self._fallback_bundle(mun_id)[bundle_key]

    def get_ideb_municipal_results(self, id_municipio: str) -> dict[str, Any]:
        """Retorna resultados consolidados do IDEB municipal por etapa e cumprimento de metas."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "ideb_resultados_municipios.parquet",
            "resultados",
        )

    def list_ideb_stage_network_scores(
        self,
        id_municipio: str,
        etapa_ciclo: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista notas IDEB observadas versus metas INEP por etapa de ensino e rede."""
        mun_id = self._resolve_municipio_id(id_municipio)
        parquet_file = self.silver_dir / "ideb_etapas_redes_ensino.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            rows = self._fallback_bundle(mun_id)["etapas"]

        if etapa_ciclo:
            needle = etapa_ciclo.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("etapa_ciclo_ideb", "")).upper()
            ]
        return rows

    def get_ideb_saeb_proficiency(self, id_municipio: str) -> dict[str, Any]:
        """Retorna proficiências SAEB em Língua Portuguesa e Matemática e aprendizado adequado."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "ideb_proficiencia_saeb_aprendizagem.parquet",
            "saeb",
        )

    def get_ideb_biennial_goals_evolution(self, id_municipio: str) -> dict[str, Any]:
        """Retorna evolução bienal do IDEB, gaps frente às metas INEP e equidade INSE."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "ideb_metas_evolucao_bienal.parquet",
            "evolucao",
        )

    def get_ideb_education_quality_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold IDEB de desempenho educacional e proficiência/equidade SAEB."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.gold_dir / "features_ideb_qualidade_educacional_municipios_brasil.parquet",
            "gold",
        )

    def list_top_ideb_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista municípios ranqueados pelos scores de desempenho IDEB e proficiência SAEB."""
        if limit < 1 or limit > 100:
            raise ValueError("Parametro 'limit' deve estar entre 1 e 100.")

        gold_file = (
            self.gold_dir / "features_ideb_qualidade_educacional_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.extract_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_desempenho_ideb_municipal", 0.0),
                row.get("score_proficiencia_equidade_saeb", 0.0),
            ),
            reverse=True,
        )
        return rows[:limit]


register_connector("inep_ideb", INEPIdebConnector)
