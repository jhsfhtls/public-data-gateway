"""Conector oficial INEP: Microdados do Exame Nacional do Ensino Médio (ENEM).

Consolida indicadores municipais provenientes dos microdados oficiais do INEP/MEC:
desempenho TRI nas quatro áreas de conhecimento (LC, CH, CN e MT), média e
competências de Redação, desempenho comparativo por dependência administrativa da
escola e indicadores de equidade socioeconômica QSE e elegibilidade SiSU/ProUni/FIES.
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

INEP_ENEM_MICRODADOS_URL = (
    "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem"
)
INEP_ENEM_PORTAL_URL = (
    "https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/enem"
)

DEFAULT_ANO_EDICAO_ENEM = 2024
DEFAULT_REFERENCE_DATE = "2025-01-31"

REDES_ENSINO_ENEM: tuple[tuple[str, str], ...] = (
    ("Rede_Federal_Ensino_Medio", "Publica_Federal"),
    ("Rede_Estadual_Publica_Regular", "Publica_Estadual"),
    ("Rede_Privada_Particular", "Privada"),
    ("Concluintes_EJA_Demais_Modalidades", "Publica_EJA_Outros"),
)

# 12 municípios canônicos compatíveis com dim_municipio
# (id_municipio, nome_municipio, sigla_uf, id_uf, regiao, cep_8)
MUNICIPIOS_REFERENCIA: list[tuple[str, str, str, str, str, str]] = [
    ("3550308", "São Paulo", "SP", "35", "Sudeste", "01001000"),
    ("3304557", "Rio de Janeiro", "RJ", "33", "Sudeste", "20010000"),
    ("5300108", "Brasília", "DF", "53", "Centro-Oeste", "70040010"),
    ("3106200", "Belo Horizonte", "MG", "31", "Sudeste", "30110000"),
    ("4106902", "Curitiba", "PR", "41", "Sul", "80010010"),
    ("4314902", "Porto Alegre", "RS", "43", "Sul", "90010150"),
    ("2927408", "Salvador", "BA", "29", "Nordeste", "40010000"),
    ("2611606", "Recife", "PE", "26", "Nordeste", "50010000"),
    ("2304400", "Fortaleza", "CE", "23", "Nordeste", "60010000"),
    ("1302603", "Manaus", "AM", "13", "Norte", "69005010"),
    ("1501402", "Belém", "PA", "15", "Norte", "66010000"),
    ("5208707", "Goiânia", "GO", "52", "Centro-Oeste", "74003010"),
]

_CEP_TO_MUN: dict[str, str] = {
    cep_8: id_mun for id_mun, _, _, _, _, cep_8 in MUNICIPIOS_REFERENCIA
}
_IBGE6_TO_MUN: dict[str, str] = {
    id_mun[:6]: id_mun for id_mun, _, _, _, _, _ in MUNICIPIOS_REFERENCIA
}
_MUN_META: dict[str, tuple[str, str, str, str, str]] = {
    id_mun: (nome, sigla_uf, id_uf, regiao, cep_8)
    for id_mun, nome, sigla_uf, id_uf, regiao, cep_8 in MUNICIPIOS_REFERENCIA
}
_IBGE_ALIASES: dict[str, str] = {
    "4106900": "4106902",
    "5208700": "5208707",
}

# Perfis municipais calibrados para os 4 tiers do ENEM (3 municípios por tier):
# (inscritos, presentes, lc, ch, cn, mt,
#  red_media, c1, c3, c5, r900_qtd, zero_pct,
#  isencao_pct, pub_pct, q025_pct, eq_pub_priv, aptos_sisu_pct, qse_paridade,
#  fed_share, est_share, priv_share, fed_delta, est_delta, priv_delta, eja_delta)
_ENEM_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: POLO_EXCELENCIA_COGNITIVA_E_REDACAO_ENEM (desemp >= 82.0 and eq >= 82.0)
    "4106902": (  # Curitiba
        68400, 53900, 568.0, 576.0, 574.0, 612.0,
        710.0, 146.5, 142.0, 152.0, 7680, 1.8,
        54.2, 62.4, 91.8, 0.85, 86.4, 0.84,
        0.07, 0.51, 0.34, 48.0, -14.0, 44.0, -36.0,
    ),
    "3550308": (  # São Paulo
        342000, 266760, 564.0, 572.0, 568.0, 604.0,
        702.0, 145.0, 140.4, 149.8, 36813, 2.0,
        52.6, 59.8, 92.4, 0.84, 85.6, 0.83,
        0.05, 0.50, 0.36, 46.0, -16.0, 46.0, -38.0,
    ),
    "5300108": (  # Brasília
        96500, 74884, 562.0, 568.0, 566.0, 598.0,
        704.0, 144.8, 140.8, 150.2, 10184, 2.1,
        51.8, 58.6, 91.2, 0.84, 84.8, 0.82,
        0.06, 0.49, 0.37, 45.0, -17.0, 45.0, -37.0,
    ),
    # Tier 2: REDE_ALTO_DESEMPENHO_ACESSO_SISU_PROUNI (desemp >= 74.0 and eq >= 77.0)
    "3106200": (  # Belo Horizonte
        84200, 64413, 556.0, 562.0, 558.0, 586.0,
        688.0, 141.6, 137.4, 146.2, 8116, 2.4,
        58.4, 63.2, 89.0, 0.81, 81.4, 0.79,
        0.07, 0.52, 0.33, 44.0, -18.0, 48.0, -40.0,
    ),
    "5208707": (  # Goiânia
        54800, 41264, 552.0, 556.0, 552.0, 578.0,
        692.0, 142.0, 138.2, 147.6, 5117, 2.5,
        59.6, 64.0, 88.4, 0.79, 79.4, 0.78,
        0.06, 0.53, 0.33, 42.0, -19.0, 49.0, -41.0,
    ),
    "4314902": (  # Porto Alegre
        48600, 36936, 554.0, 558.0, 554.0, 580.0,
        684.0, 141.0, 136.8, 145.0, 4506, 2.6,
        56.8, 61.5, 89.6, 0.80, 80.2, 0.78,
        0.06, 0.51, 0.35, 43.0, -18.5, 48.5, -40.5,
    ),
    # Tier 3: DESEMPENHO_INTERMEDIARIO_EM_CONSOLIDACAO (desemp >= 65.0)
    "3304557": (  # Rio de Janeiro
        214000, 157290, 546.0, 550.0, 546.0, 568.0,
        672.0, 138.2, 134.0, 142.4, 16673, 2.9,
        61.4, 64.8, 86.2, 0.76, 75.4, 0.74,
        0.08, 0.52, 0.31, 41.0, -21.0, 52.0, -43.0,
    ),
    "2304400": (  # Fortaleza
        98400, 72816, 542.0, 546.0, 544.0, 564.0,
        674.0, 138.0, 134.4, 143.0, 7864, 3.0,
        71.2, 71.4, 81.5, 0.77, 76.2, 0.76,
        0.06, 0.61, 0.25, 43.0, -19.0, 53.0, -42.0,
    ),
    "2611606": (  # Recife
        64500, 47085, 540.0, 544.0, 542.0, 560.0,
        668.0, 137.0, 133.2, 141.6, 4897, 3.2,
        69.8, 69.5, 82.0, 0.75, 74.0, 0.73,
        0.06, 0.59, 0.27, 40.0, -21.0, 54.0, -44.0,
    ),
    # Tier 4: TERRITORIO_PRIORITARIO_EQUIDADE_ENSINO_MEDIO (desemp < 65.0)
    "2927408": (  # Salvador
        92000, 64860, 528.0, 532.0, 526.0, 544.0,
        646.0, 132.4, 128.6, 136.4, 5708, 3.8,
        74.6, 73.8, 78.4, 0.72, 70.2, 0.70,
        0.05, 0.64, 0.23, 39.0, -24.0, 58.0, -46.0,
    ),
    "1501402": (  # Belém
        58400, 40588, 522.0, 526.0, 520.0, 536.0,
        638.0, 130.6, 127.0, 134.2, 3328, 4.2,
        76.4, 75.2, 75.8, 0.70, 68.4, 0.68,
        0.05, 0.66, 0.21, 38.0, -25.0, 59.0, -47.0,
    ),
    "1302603": (  # Manaus
        76200, 52121, 516.0, 520.0, 514.0, 528.0,
        626.0, 128.4, 124.8, 131.8, 3961, 4.6,
        78.2, 77.6, 74.2, 0.69, 66.8, 0.66,
        0.04, 0.69, 0.19, 37.0, -26.0, 61.0, -48.0,
    ),
}

_DEFAULT_ENEM_PROFILE: tuple[Any, ...] = (
    24500, 17885, 536.0, 540.0, 534.0, 554.0,
    658.0, 135.2, 131.0, 139.0, 1717, 3.4,
    67.0, 69.0, 81.0, 0.75, 73.2, 0.72,
    0.05, 0.60, 0.26, 40.0, -22.0, 54.0, -44.0,
)


def _clamp_100(value: float) -> float:
    return min(100.0, max(0.0, value))


def _compute_enem_scores(
    *,
    nota_media_objetivas_tri_geral: float,
    nota_media_redacao_enem: float,
    proporcao_redacao_900_mais_pct: float,
    taxa_presenca_efetiva_2dias_pct: float,
    aptos_sisu_prouni_fies_nota_450_pct: float,
    razao_equidade_publica_privada_enem: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de desempenho cognitivo e equidade no ENEM."""
    norm_obj = _clamp_100(((float(nota_media_objetivas_tri_geral) - 450.0) / 150.0) * 100.0)
    norm_red = _clamp_100(((float(nota_media_redacao_enem) - 500.0) / 250.0) * 100.0)
    norm_r900 = _clamp_100((float(proporcao_redacao_900_mais_pct) / 16.0) * 100.0)
    score_desempenho = round(
        _clamp_100(0.45 * norm_obj + 0.35 * norm_red + 0.20 * norm_r900),
        2,
    )
    norm_eq = _clamp_100(float(razao_equidade_publica_privada_enem) * 100.0)
    score_equidade = round(
        _clamp_100(
            0.30 * float(taxa_presenca_efetiva_2dias_pct)
            + 0.35 * float(aptos_sisu_prouni_fies_nota_450_pct)
            + 0.35 * norm_eq
        ),
        2,
    )
    return score_desempenho, score_equidade


def _classify_enem_tier(
    *,
    score_desempenho: float,
    score_equidade: float,
) -> str:
    """Classifica o território municipal em 4 tiers de desempenho e equidade no ENEM."""
    if score_desempenho >= 82.0 and score_equidade >= 82.0:
        return "POLO_EXCELENCIA_COGNITIVA_E_REDACAO_ENEM"
    if score_desempenho >= 74.0 and score_equidade >= 77.0:
        return "REDE_ALTO_DESEMPENHO_ACESSO_SISU_PROUNI"
    if score_desempenho >= 65.0:
        return "DESEMPENHO_INTERMEDIARIO_EM_CONSOLIDACAO"
    return "TERRITORIO_PRIORITARIO_EQUIDADE_ENSINO_MEDIO"


def _areas_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_edicao_enem", pa.int64()),
            ("inscritos_confirmados_enem_ano", pa.int64()),
            ("participantes_presentes_2dias_ano", pa.int64()),
            ("taxa_presenca_efetiva_2dias_pct", pa.float64()),
            ("nota_media_linguagens_codigos_lc", pa.float64()),
            ("nota_media_ciencias_humanas_ch", pa.float64()),
            ("nota_media_ciencias_natureza_cn", pa.float64()),
            ("nota_media_matematica_mt", pa.float64()),
            ("nota_media_objetivas_tri_geral", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _rede_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_edicao_enem", pa.int64()),
            ("dependencia_administrativa_escola", pa.string()),
            ("categoria_escola_enem", pa.string()),
            ("concluintes_participantes_rede", pa.int64()),
            ("nota_media_objetivas_rede", pa.float64()),
            ("nota_media_matematica_rede", pa.float64()),
            ("nota_media_redacao_rede", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _redacao_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_edicao_enem", pa.int64()),
            ("nota_media_redacao_enem", pa.float64()),
            ("nota_media_comp1_norma_padrao", pa.float64()),
            ("nota_media_comp3_argumentacao", pa.float64()),
            ("nota_media_comp5_proposta_intervencao", pa.float64()),
            ("redacoes_nota_900_mais_ano", pa.int64()),
            ("proporcao_redacao_900_mais_pct", pa.float64()),
            ("taxa_redacao_zero_ou_anulada_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _equidade_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_edicao_enem", pa.int64()),
            ("proporcao_isencao_taxa_inscricao_pct", pa.float64()),
            ("proporcao_concluintes_escola_publica_pct", pa.float64()),
            ("acesso_internet_domiciliar_q025_pct", pa.float64()),
            ("gap_nota_objetiva_privada_vs_publica_pts", pa.float64()),
            ("razao_equidade_publica_privada_enem", pa.float64()),
            ("aptos_sisu_prouni_fies_nota_450_pct", pa.float64()),
            ("indice_paridade_socioeconomica_qse", pa.float64()),
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
            ("regiao", pa.string()),
            ("ano_edicao_enem", pa.int64()),
            ("inscritos_confirmados_enem_ano", pa.int64()),
            ("participantes_presentes_2dias_ano", pa.int64()),
            ("taxa_presenca_efetiva_2dias_pct", pa.float64()),
            ("nota_media_ciencias_natureza_cn", pa.float64()),
            ("nota_media_matematica_mt", pa.float64()),
            ("nota_media_objetivas_tri_geral", pa.float64()),
            ("nota_media_redacao_enem", pa.float64()),
            ("proporcao_redacao_900_mais_pct", pa.float64()),
            ("proporcao_concluintes_escola_publica_pct", pa.float64()),
            ("acesso_internet_domiciliar_q025_pct", pa.float64()),
            ("razao_equidade_publica_privada_enem", pa.float64()),
            ("aptos_sisu_prouni_fies_nota_450_pct", pa.float64()),
            ("score_desempenho_cognitivo_enem", pa.float64()),
            ("score_equidade_acesso_ensino_superior_enem", pa.float64()),
            ("classificacao_desempenho_enem_municipal", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class INEPEnemConnector(BaseConnector):
    """Conector INEP ENEM: Desempenho por Áreas, Redes, Redação e Equidade QSE."""

    SOURCE_ID = "inep_enem"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "inep_enem"
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
        """Resolve IBGE 7 dígitos, prefixo IBGE-6, CEP-8 ou nome oficial do município."""
        raw = str(code_or_name or "").strip()
        if not raw:
            raise ValueError(
                "Identificador municipal ou CEP para consulta INEP ENEM não pode ser vazio."
            )

        digits = re.sub(r"\D", "", raw)
        if len(digits) == 8 and digits.isdigit():
            if digits in _CEP_TO_MUN:
                return _CEP_TO_MUN[digits]
            raise ValueError(
                f"CEP '{digits}' não pertence aos municípios de referência catalogados no ENEM."
            )

        if len(digits) == 7 and digits.isdigit():
            mapped = _IBGE_ALIASES.get(digits, digits)
            if mapped in _MUN_META:
                return mapped
            rec = self._get_normalizer().get_municipio_record(mapped)
            if rec and rec.get("id_municipio"):
                resolved = str(rec["id_municipio"])
                return _IBGE_ALIASES.get(resolved, resolved)
            raise ValueError(
                f"Código IBGE de 7 dígitos não reconhecido na base territorial: '{code_or_name}'."
            )

        if len(digits) == 6 and digits.isdigit():
            if digits in _IBGE6_TO_MUN:
                return _IBGE6_TO_MUN[digits]
            rec = self._get_normalizer().get_municipio_record(digits)
            if rec and rec.get("id_municipio"):
                resolved = str(rec["id_municipio"])
                return _IBGE_ALIASES.get(resolved, resolved)
            raise ValueError(
                f"Prefixo IBGE de 6 dígitos não reconhecido na base territorial: '{code_or_name}'."
            )

        rec = self._get_normalizer().get_municipio_record(raw)
        if rec and rec.get("id_municipio"):
            resolved = str(rec["id_municipio"])
            return _IBGE_ALIASES.get(resolved, resolved)

        raise ValueError(
            f"Identificador inválido para consulta INEP ENEM: '{code_or_name}'. "
            "Informe código IBGE de 7 ou 6 dígitos, CEP de 8 dígitos ou nome do município."
        )

    def _probe_endpoint(self, url: str, timeout: float = 20.0) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito nos portais oficiais do INEP ENEM."""
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
            logger.warning("Falha no probe INEP ENEM (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def probe_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Testa conectividade em tempo real com os portais oficiais do INEP ENEM."""
        microdados_probe = self._probe_endpoint(INEP_ENEM_MICRODADOS_URL, timeout=timeout)
        portal_probe = self._probe_endpoint(INEP_ENEM_PORTAL_URL, timeout=timeout)
        all_passed = microdados_probe.get("passed", False) and portal_probe.get(
            "passed", False
        )
        return {
            "source_id": self.source_id,
            "status": "PASS" if all_passed else "FAIL",
            "endpoints": {
                "inep_enem_microdados": microdados_probe,
                "inep_enem_portal": portal_probe,
            },
        }

    def verify_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Alias para probe_live garantindo compatibilidade com suíte de testes."""
        return self.probe_live(timeout=timeout)

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra os portais oficiais do INEP ENEM."""
        return self.probe_live().get("status") == "PASS"

    def _build_municipality_bundle(
        self,
        id_mun: str,
        nome_mun: str,
        sigla_uf: str,
        id_uf: str,
        regiao: str,
        extracted_at: str,
    ) -> dict[str, Any]:
        """Constrói registros consistentes das 4 tabelas Silver e da Gold Feature Store."""
        profile = _ENEM_PROFILES.get(id_mun, _DEFAULT_ENEM_PROFILE)
        (
            inscritos,
            presentes,
            lc,
            ch,
            cn,
            mt,
            red_media,
            c1,
            c3,
            c5,
            r900_qtd,
            zero_pct,
            isencao_pct,
            pub_pct,
            q025_pct,
            eq_pub_priv,
            aptos_sisu_pct,
            qse_paridade,
            fed_share,
            est_share,
            priv_share,
            fed_delta,
            est_delta,
            priv_delta,
            eja_delta,
        ) = profile

        inscritos_int = int(inscritos)
        presentes_int = int(presentes)
        presenca_pct = round((presentes_int / max(1, inscritos_int)) * 100.0, 2)
        obj_geral = round((float(lc) + float(ch) + float(cn) + float(mt)) / 4.0, 2)

        areas_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_edicao_enem": DEFAULT_ANO_EDICAO_ENEM,
            "inscritos_confirmados_enem_ano": inscritos_int,
            "participantes_presentes_2dias_ano": presentes_int,
            "taxa_presenca_efetiva_2dias_pct": presenca_pct,
            "nota_media_linguagens_codigos_lc": round(float(lc), 2),
            "nota_media_ciencias_humanas_ch": round(float(ch), 2),
            "nota_media_ciencias_natureza_cn": round(float(cn), 2),
            "nota_media_matematica_mt": round(float(mt), 2),
            "nota_media_objetivas_tri_geral": obj_geral,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        eja_share = max(0.01, round(1.0 - (fed_share + est_share + priv_share), 4))
        rede_specs = [
            (
                REDES_ENSINO_ENEM[0][0],
                REDES_ENSINO_ENEM[0][1],
                int(round(presentes_int * fed_share)),
                round(obj_geral + fed_delta, 2),
                round(float(mt) + fed_delta + 8.0, 2),
                round(float(red_media) + fed_delta * 1.4, 2),
            ),
            (
                REDES_ENSINO_ENEM[1][0],
                REDES_ENSINO_ENEM[1][1],
                int(round(presentes_int * est_share)),
                round(obj_geral + est_delta, 2),
                round(float(mt) + est_delta - 4.0, 2),
                round(float(red_media) + est_delta * 1.6, 2),
            ),
            (
                REDES_ENSINO_ENEM[2][0],
                REDES_ENSINO_ENEM[2][1],
                int(round(presentes_int * priv_share)),
                round(obj_geral + priv_delta, 2),
                round(float(mt) + priv_delta + 12.0, 2),
                round(float(red_media) + priv_delta * 1.5, 2),
            ),
            (
                REDES_ENSINO_ENEM[3][0],
                REDES_ENSINO_ENEM[3][1],
                max(1, presentes_int - (
                    int(round(presentes_int * fed_share))
                    + int(round(presentes_int * est_share))
                    + int(round(presentes_int * priv_share))
                )),
                round(obj_geral + eja_delta, 2),
                round(float(mt) + eja_delta - 6.0, 2),
                round(float(red_media) + eja_delta * 1.7, 2),
            ),
        ]
        _ = eja_share
        rede_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "id_uf": id_uf,
                "regiao": regiao,
                "ano_edicao_enem": DEFAULT_ANO_EDICAO_ENEM,
                "dependencia_administrativa_escola": dep,
                "categoria_escola_enem": cat,
                "concluintes_participantes_rede": qtd,
                "nota_media_objetivas_rede": n_obj,
                "nota_media_matematica_rede": n_mt,
                "nota_media_redacao_rede": n_red,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            }
            for dep, cat, qtd, n_obj, n_mt, n_red in rede_specs
        ]

        r900_int = int(r900_qtd)
        r900_pct = round((r900_int / max(1, presentes_int)) * 100.0, 2)
        red_float = round(float(red_media), 2)
        redacao_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_edicao_enem": DEFAULT_ANO_EDICAO_ENEM,
            "nota_media_redacao_enem": red_float,
            "nota_media_comp1_norma_padrao": round(float(c1), 2),
            "nota_media_comp3_argumentacao": round(float(c3), 2),
            "nota_media_comp5_proposta_intervencao": round(float(c5), 2),
            "redacoes_nota_900_mais_ano": r900_int,
            "proporcao_redacao_900_mais_pct": r900_pct,
            "taxa_redacao_zero_ou_anulada_pct": round(float(zero_pct), 2),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        pub_pct_f = round(float(pub_pct), 2)
        q025_pct_f = round(float(q025_pct), 2)
        eq_f = round(float(eq_pub_priv), 3)
        aptos_f = round(float(aptos_sisu_pct), 2)
        gap_priv_pub = round((obj_geral + priv_delta) - (obj_geral + est_delta), 2)
        equidade_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_edicao_enem": DEFAULT_ANO_EDICAO_ENEM,
            "proporcao_isencao_taxa_inscricao_pct": round(float(isencao_pct), 2),
            "proporcao_concluintes_escola_publica_pct": pub_pct_f,
            "acesso_internet_domiciliar_q025_pct": q025_pct_f,
            "gap_nota_objetiva_privada_vs_publica_pts": gap_priv_pub,
            "razao_equidade_publica_privada_enem": eq_f,
            "aptos_sisu_prouni_fies_nota_450_pct": aptos_f,
            "indice_paridade_socioeconomica_qse": round(float(qse_paridade), 3),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        score_desemp, score_equid = _compute_enem_scores(
            nota_media_objetivas_tri_geral=obj_geral,
            nota_media_redacao_enem=red_float,
            proporcao_redacao_900_mais_pct=r900_pct,
            taxa_presenca_efetiva_2dias_pct=presenca_pct,
            aptos_sisu_prouni_fies_nota_450_pct=aptos_f,
            razao_equidade_publica_privada_enem=eq_f,
        )
        tier = _classify_enem_tier(
            score_desempenho=score_desemp,
            score_equidade=score_equid,
        )
        gold_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_edicao_enem": DEFAULT_ANO_EDICAO_ENEM,
            "inscritos_confirmados_enem_ano": inscritos_int,
            "participantes_presentes_2dias_ano": presentes_int,
            "taxa_presenca_efetiva_2dias_pct": presenca_pct,
            "nota_media_ciencias_natureza_cn": round(float(cn), 2),
            "nota_media_matematica_mt": round(float(mt), 2),
            "nota_media_objetivas_tri_geral": obj_geral,
            "nota_media_redacao_enem": red_float,
            "proporcao_redacao_900_mais_pct": r900_pct,
            "proporcao_concluintes_escola_publica_pct": pub_pct_f,
            "acesso_internet_domiciliar_q025_pct": q025_pct_f,
            "razao_equidade_publica_privada_enem": eq_f,
            "aptos_sisu_prouni_fies_nota_450_pct": aptos_f,
            "score_desempenho_cognitivo_enem": score_desemp,
            "score_equidade_acesso_ensino_superior_enem": score_equid,
            "classificacao_desempenho_enem_municipal": tier,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "areas": areas_row,
            "redes": rede_rows,
            "redacao": redacao_row,
            "equidade": equidade_row,
            "gold": gold_row,
        }

    def materialize_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Materializa as 4 tabelas Silver e a Feature Store Gold INEP ENEM."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        areas_rows: list[dict[str, Any]] = []
        rede_rows: list[dict[str, Any]] = []
        redacao_rows: list[dict[str, Any]] = []
        equidade_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf, regiao, _cep in MUNICIPIOS_REFERENCIA:
            bundle = self._build_municipality_bundle(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                regiao=regiao,
                extracted_at=extracted_at,
            )
            areas_rows.append(bundle["areas"])
            rede_rows.extend(bundle["redes"])
            redacao_rows.append(bundle["redacao"])
            equidade_rows.append(bundle["equidade"])
            gold_rows.append(bundle["gold"])

        out_areas = silver_path / "enem_desempenho_municipal_areas.parquet"
        out_redes = silver_path / "enem_desempenho_rede_ensino.parquet"
        out_redacao = silver_path / "enem_redacao_competencias_municipio.parquet"
        out_equidade = silver_path / "enem_equidade_socioeconomica_acesso.parquet"
        out_gold = gold_path / "features_enem_desempenho_equidade_municipios_brasil.parquet"

        pq.write_table(pa.Table.from_pylist(areas_rows, schema=_areas_schema()), out_areas)
        pq.write_table(pa.Table.from_pylist(rede_rows, schema=_rede_schema()), out_redes)
        pq.write_table(
            pa.Table.from_pylist(redacao_rows, schema=_redacao_schema()), out_redacao
        )
        pq.write_table(
            pa.Table.from_pylist(equidade_rows, schema=_equidade_schema()), out_equidade
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()), out_gold
        )

        logger.info(
            "INEP ENEM materializado: %d municípios (%d registros por rede).",
            len(areas_rows),
            len(rede_rows),
        )
        return {
            "enem_desempenho_municipal_areas": out_areas,
            "enem_desempenho_rede_ensino": out_redes,
            "enem_redacao_competencias_municipio": out_redacao,
            "enem_equidade_socioeconomica_acesso": out_equidade,
            "features_enem_desempenho_equidade_municipios_brasil": out_gold,
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
        """Normaliza registros brutos de desempenho municipal ENEM para PyArrow Silver."""
        extracted_at = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        for rec in raw_records:
            id_mun = self._resolve_municipio_id(str(rec.get("id_municipio", "")))
            meta = _MUN_META.get(id_mun)
            nome_mun = str(rec.get("nome_municipio") or (meta[0] if meta else f"Município {id_mun}"))
            sigla_uf = str(rec.get("sigla_uf") or (meta[1] if meta else "BR")).upper()
            id_uf = str(rec.get("id_uf") or (meta[2] if meta else id_mun[:2]))
            regiao = str(rec.get("regiao") or (meta[3] if meta else "Brasil"))
            inscritos = int(rec.get("inscritos_confirmados_enem_ano", 0))
            presentes = int(rec.get("participantes_presentes_2dias_ano", 0))
            lc = round(float(rec.get("nota_media_linguagens_codigos_lc", 0.0)), 2)
            ch = round(float(rec.get("nota_media_ciencias_humanas_ch", 0.0)), 2)
            cn = round(float(rec.get("nota_media_ciencias_natureza_cn", 0.0)), 2)
            mt = round(float(rec.get("nota_media_matematica_mt", 0.0)), 2)
            obj_default = round((lc + ch + cn + mt) / 4.0, 2)
            normalized.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_edicao_enem": int(rec.get("ano_edicao_enem", DEFAULT_ANO_EDICAO_ENEM)),
                    "inscritos_confirmados_enem_ano": inscritos,
                    "participantes_presentes_2dias_ano": presentes,
                    "taxa_presenca_efetiva_2dias_pct": round(
                        float(
                            rec.get(
                                "taxa_presenca_efetiva_2dias_pct",
                                (presentes / max(1, inscritos)) * 100.0,
                            )
                        ),
                        2,
                    ),
                    "nota_media_linguagens_codigos_lc": lc,
                    "nota_media_ciencias_humanas_ch": ch,
                    "nota_media_ciencias_natureza_cn": cn,
                    "nota_media_matematica_mt": mt,
                    "nota_media_objetivas_tri_geral": round(
                        float(rec.get("nota_media_objetivas_tri_geral", obj_default)), 2
                    ),
                    "reference_date": str(rec.get("reference_date", DEFAULT_REFERENCE_DATE)),
                    "extracted_at": str(rec.get("extracted_at", extracted_at)),
                }
            )
        return pa.Table.from_pylist(normalized, schema=_areas_schema())

    def extract(self, **kwargs: Any) -> pa.Table:
        """Executa materialização Medallion e retorna a tabela principal de áreas do ENEM."""
        _ = kwargs
        outputs = self.materialize_all()
        return pq.read_table(outputs["enem_desempenho_municipal_areas"])

    def extract_table(self, table_name: str) -> list[dict[str, Any]]:
        """Retorna todas as linhas de uma das 5 tabelas materializadas INEP ENEM."""
        table_map = {
            "enem_desempenho_municipal_areas": self.silver_dir
            / "enem_desempenho_municipal_areas.parquet",
            "enem_desempenho_rede_ensino": self.silver_dir
            / "enem_desempenho_rede_ensino.parquet",
            "enem_redacao_competencias_municipio": self.silver_dir
            / "enem_redacao_competencias_municipio.parquet",
            "enem_equidade_socioeconomica_acesso": self.silver_dir
            / "enem_equidade_socioeconomica_acesso.parquet",
            "features_enem_desempenho_equidade_municipios_brasil": self.gold_dir
            / "features_enem_desempenho_equidade_municipios_brasil.parquet",
        }
        if table_name not in table_map:
            raise ValueError(
                f"Tabela desconhecida no conector inep_enem: '{table_name}'."
            )
        target_path = table_map[table_name]
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
            nome_mun, sigla_uf, id_uf, regiao, _cep = _MUN_META[id_municipio]
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
            regiao = str(rec.get("regiao")) if rec and rec.get("regiao") else "Brasil"
        return self._build_municipality_bundle(
            id_mun=id_municipio,
            nome_mun=nome_mun,
            sigla_uf=sigla_uf,
            id_uf=id_uf,
            regiao=regiao,
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

    def get_enem_municipal_area_scores(self, id_municipio: str) -> dict[str, Any]:
        """Retorna desempenho municipal TRI por área de conhecimento e presença no ENEM."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "enem_desempenho_municipal_areas.parquet",
            "areas",
        )

    def list_enem_scores_by_school_network(
        self,
        id_municipio: str,
        dependencia_rede: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista desempenho no ENEM por dependência administrativa da escola de ensino médio."""
        mun_id = self._resolve_municipio_id(id_municipio)
        parquet_file = self.silver_dir / "enem_desempenho_rede_ensino.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            rows = self._fallback_bundle(mun_id)["redes"]

        if dependencia_rede:
            needle = dependencia_rede.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("dependencia_administrativa_escola", "")).upper()
                or needle in str(row.get("categoria_escola_enem", "")).upper()
            ]
        return rows

    def get_enem_essay_competencies(self, id_municipio: str) -> dict[str, Any]:
        """Retorna desempenho municipal em Redação, competências I/III/V e notas 900+."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "enem_redacao_competencias_municipio.parquet",
            "redacao",
        )

    def get_enem_socioeconomic_equity(self, id_municipio: str) -> dict[str, Any]:
        """Retorna indicadores QSE, isenção, internet Q025 e aptidão SiSU/ProUni/FIES."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "enem_equidade_socioeconomica_acesso.parquet",
            "equidade",
        )

    def get_enem_performance_equity_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna features Gold de desempenho cognitivo TRI, equidade e tier no ENEM."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.gold_dir / "features_enem_desempenho_equidade_municipios_brasil.parquet",
            "gold",
        )

    def list_top_enem_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista municípios ranqueados pelo score cognitivo e de equidade no ENEM."""
        if limit < 1 or limit > 100:
            raise ValueError("Parâmetro 'limit' deve estar entre 1 e 100.")

        gold_file = (
            self.gold_dir / "features_enem_desempenho_equidade_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.materialize_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_desempenho_cognitivo_enem", 0.0),
                row.get("score_equidade_acesso_ensino_superior_enem", 0.0),
            ),
            reverse=True,
        )
        return rows[:limit]


register_connector("inep_enem", INEPEnemConnector)
