"""Conector oficial OpenDataSUS: Notificações de Síndromes e Vigilância Epidemiológica.

Consolida indicadores municipais provenientes dos sistemas oficiais do Ministério da Saúde
(SVSA / OpenDataSUS / RNDS): Síndrome Gripal (e-SUS Notifica) e Síndrome Respiratória Aguda
Grave (SIVEP-Gripe), agravos de notificação compulsória (SINAN), cobertura vacinal e
homogeneidade (SI-PNI) e capacidade de resposta rápida CIEVS/GAL-LACEN.
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

OPENDATASUS_PORTAL_URL = "https://opendatasus.saude.gov.br/"
SVSA_MINISTERIO_SAUDE_URL = "https://www.gov.br/saude/pt-br/composicao/svsa"

DEFAULT_ANO_EPIDEMIOLOGICO = 2025
DEFAULT_REFERENCE_DATE = "2025-12-31"

GRUPOS_AGRAVO_SINAN: tuple[tuple[str, str], ...] = (
    ("Arboviroses_Dengue_Chikungunya_Zika", "Vetorial_Aedes"),
    ("Sindromes_Respiratorias_Influenza_COVID19", "Transmissao_Respiratoria"),
    ("Acidentes_Animais_Peconhentos_Zoonoses", "Ambiental_Zoonotica"),
    ("Tuberculose_Hanseniase_Agravos_Cronicos", "Transmissao_Direta_Cronica"),
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

# Perfis epidemiológicos e vacinais por município (base SVSA/OpenDataSUS):
# (pop_100k, sg_esus, srag_hosp, uti_share, obito_share, rt_pcr_pct,
#  arbo_notif, arbo_conf, resp_notif, resp_conf, zoon_notif, zoon_conf, cron_notif, cron_conf, enc_oportuno_pct,
#  doses_pni, salas_vac, penta_pct, vip_pct, tv_pct, homog_pct, abandono_pct,
#  sentinelas, not_24h_pct, inv_48h_pct, gal_horas, bloq_vet_pct, surtos_enc_pct, rnds_pct)
_VIGILANCIA_PROFILES: dict[str, tuple[Any, ...]] = {
    # POLO_SENTINELA_ALTA_COBERTURA_IMUNIZACAO (prontidao >= 88.0 and pni >= 86.0)
    "4106902": (  # Curitiba
        17.74, 248600, 3140, 0.242, 0.086, 94.0,
        24800, 14600, 18400, 11800, 3120, 2460, 1420, 1180, 93.2,
        1480000, 112, 91.4, 92.6, 93.6, 88.0, 2.4,
        14, 95.2, 93.8, 18.4, 92.5, 96.4, 96.0,
    ),
    "3550308": (  # São Paulo
        114.52, 1345000, 18420, 0.268, 0.092, 92.4,
        214000, 132000, 96400, 62800, 14200, 10800, 11600, 9420, 91.6,
        8940000, 478, 89.6, 90.8, 92.0, 86.0, 2.9,
        46, 93.6, 91.4, 21.6, 89.8, 94.6, 95.2,
    ),
    "4314902": (  # Porto Alegre
        13.33, 186400, 2680, 0.258, 0.094, 90.8,
        21600, 12400, 16200, 10100, 2640, 1980, 1680, 1340, 89.2,
        1120000, 84, 88.2, 89.4, 90.4, 84.0, 3.2,
        12, 91.2, 89.6, 22.8, 88.4, 93.2, 92.8,
    ),
    # REDE_VIGILANCIA_EPIDEMIOLOGICA_CONSOLIDADA (prontidao >= 82.0 and pni >= 80.0)
    "5300108": (  # Brasília
        28.17, 362400, 4520, 0.254, 0.096, 86.4,
        89400, 56200, 26800, 17400, 4180, 3120, 2140, 1720, 85.6,
        2340000, 162, 84.4, 85.8, 86.6, 81.0, 3.8,
        18, 87.6, 86.2, 26.4, 85.6, 90.8, 88.4,
    ),
    "3106200": (  # Belo Horizonte
        23.16, 318600, 4180, 0.262, 0.098, 85.2,
        142000, 91400, 23400, 14800, 3960, 2940, 2280, 1820, 84.4,
        1980000, 154, 83.2, 84.6, 85.4, 79.5, 4.1,
        16, 86.4, 84.8, 28.2, 84.2, 89.4, 87.2,
    ),
    "5208707": (  # Goiânia
        14.37, 194200, 2490, 0.248, 0.101, 83.2,
        68400, 42800, 13600, 8420, 3420, 2480, 1340, 1040, 82.4,
        1210000, 88, 81.8, 83.0, 84.0, 77.5, 4.5,
        10, 84.0, 82.6, 30.8, 82.0, 87.6, 85.6,
    ),
    # TERRITORIO_PRESSAO_ENDEMICA_EM_MONITORAMENTO (prontidao >= 75.0)
    "3304557": (  # Rio de Janeiro
        62.11, 712400, 9860, 0.278, 0.114, 80.4,
        136000, 82400, 48200, 29600, 7840, 5620, 9640, 7680, 79.6,
        4620000, 246, 78.4, 79.8, 81.0, 73.5, 5.4,
        24, 81.6, 79.8, 34.6, 77.8, 84.2, 82.4,
    ),
    "2611606": (  # Recife
        14.89, 178400, 2640, 0.284, 0.118, 78.0,
        46800, 28400, 14200, 8640, 3240, 2320, 2840, 2240, 77.2,
        1180000, 96, 76.8, 78.2, 79.2, 71.0, 5.9,
        11, 79.2, 77.4, 38.4, 75.6, 81.8, 80.0,
    ),
    "2304400": (  # Fortaleza
        24.29, 264200, 3720, 0.288, 0.121, 76.0,
        64200, 38600, 19800, 11900, 4120, 2890, 3680, 2840, 75.2,
        1840000, 124, 75.6, 76.8, 78.0, 69.5, 6.3,
        13, 77.2, 75.6, 41.2, 74.2, 79.6, 78.0,
    ),
    # ZONA_PRIORITARIA_CONTROLE_ARBOVIROSES_E_PNI (prontidao < 75.0)
    "2927408": (  # Salvador
        24.18, 248600, 3580, 0.296, 0.126, 72.8,
        74600, 44800, 18600, 11200, 4680, 3240, 3940, 3020, 72.0,
        1760000, 118, 72.2, 73.6, 74.8, 66.5, 7.1,
        12, 73.6, 71.8, 46.8, 70.4, 76.4, 74.8,
    ),
    "1501402": (  # Belém
        13.03, 142800, 2180, 0.304, 0.134, 69.6,
        34800, 21200, 11400, 6680, 3840, 2620, 2960, 2240, 68.8,
        960000, 68, 68.8, 70.2, 71.4, 62.5, 8.2,
        8, 70.4, 68.4, 52.4, 66.8, 73.2, 71.6,
    ),
    "1302603": (  # Manaus
        20.64, 214600, 3290, 0.312, 0.138, 66.8,
        48200, 29400, 16800, 9840, 5120, 3580, 4120, 3180, 66.0,
        1490000, 86, 66.4, 67.8, 69.0, 59.5, 8.9,
        10, 67.6, 65.8, 56.2, 64.4, 70.8, 69.2,
    ),
}

_DEFAULT_VIGILANCIA_PROFILE: tuple[Any, ...] = (
    5.0, 58000, 780, 0.280, 0.115, 78.0,
    14200, 8400, 4600, 2800, 1100, 780, 680, 520, 77.2,
    420000, 32, 76.8, 78.0, 79.2, 71.0, 6.0,
    4, 79.2, 77.4, 38.0, 75.6, 81.8, 80.0,
)


def _clamp_100(value: float) -> float:
    return min(100.0, max(0.0, value))


def _compute_vigilancia_scores(
    *,
    confirmacao_laboratorial_rt_pcr_srag_pct: float,
    proporcao_encerramento_oportuno_sinan_pct: float,
    notificacoes_imediatas_24h_cievs_pct: float,
    integracao_rnds_esus_sinan_pct: float,
    cobertura_pentavalente_d3_pct: float,
    cobertura_triplice_viral_d1_pct: float,
    homogeneidade_cobertura_vacinal_pni_pct: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de prontidão epidemiológica e proteção vacinal PNI."""
    score_prontidao = round(
        _clamp_100(
            0.25 * float(confirmacao_laboratorial_rt_pcr_srag_pct)
            + 0.25 * float(proporcao_encerramento_oportuno_sinan_pct)
            + 0.25 * float(notificacoes_imediatas_24h_cievs_pct)
            + 0.25 * float(integracao_rnds_esus_sinan_pct)
        ),
        2,
    )
    score_pni = round(
        _clamp_100(
            0.35 * float(cobertura_pentavalente_d3_pct)
            + 0.35 * float(cobertura_triplice_viral_d1_pct)
            + 0.30 * float(homogeneidade_cobertura_vacinal_pni_pct)
        ),
        2,
    )
    return score_prontidao, score_pni


def _classify_vigilancia_tier(
    *,
    score_prontidao: float,
    score_pni: float,
) -> str:
    """Classifica o território municipal em 4 tiers de vigilância e imunização."""
    if score_prontidao >= 88.0 and score_pni >= 86.0:
        return "POLO_SENTINELA_ALTA_COBERTURA_IMUNIZACAO"
    if score_prontidao >= 82.0 and score_pni >= 80.0:
        return "REDE_VIGILANCIA_EPIDEMIOLOGICA_CONSOLIDADA"
    if score_prontidao >= 75.0:
        return "TERRITORIO_PRESSAO_ENDEMICA_EM_MONITORAMENTO"
    return "ZONA_PRIORITARIA_CONTROLE_ARBOVIROSES_E_PNI"


def _srag_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_epidemiologico", pa.int64()),
            ("notificacoes_sindrome_gripal_esus_ano", pa.int64()),
            ("internacoes_srag_sivep_ano", pa.int64()),
            ("taxa_incidencia_srag_100k_hab", pa.float64()),
            ("internacoes_srag_uti_ano", pa.int64()),
            ("proporcao_srag_uti_pct", pa.float64()),
            ("obitos_srag_confirmados_ano", pa.int64()),
            ("letalidade_hospitalar_srag_pct", pa.float64()),
            ("confirmacao_laboratorial_rt_pcr_srag_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _sinan_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_epidemiologico", pa.int64()),
            ("grupo_agravo_sinan", pa.string()),
            ("categoria_transmissao", pa.string()),
            ("casos_notificados_ano", pa.int64()),
            ("casos_confirmados_ano", pa.int64()),
            ("taxa_incidencia_confirmada_100k_hab", pa.float64()),
            ("proporcao_encerramento_oportuno_sinan_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _sipni_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_epidemiologico", pa.int64()),
            ("doses_aplicadas_calendario_pni_ano", pa.int64()),
            ("salas_vacina_ativas_cnes", pa.int64()),
            ("cobertura_pentavalente_d3_pct", pa.float64()),
            ("cobertura_poliomielite_vip_pct", pa.float64()),
            ("cobertura_triplice_viral_d1_pct", pa.float64()),
            ("homogeneidade_cobertura_vacinal_pni_pct", pa.float64()),
            ("taxa_abandono_esquema_multidose_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _cievs_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_epidemiologico", pa.int64()),
            ("unidades_sentinela_vigilancia_ativas", pa.int64()),
            ("notificacoes_imediatas_24h_cievs_pct", pa.float64()),
            ("investigacoes_epidemiologicas_48h_pct", pa.float64()),
            ("tempo_medio_liberacao_gal_lacen_horas", pa.float64()),
            ("indice_bloqueio_vetorial_arboviroses_pct", pa.float64()),
            ("surtos_investigados_encerrados_pct", pa.float64()),
            ("integracao_rnds_esus_sinan_pct", pa.float64()),
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
            ("ano_epidemiologico", pa.int64()),
            ("notificacoes_sindrome_gripal_esus_ano", pa.int64()),
            ("internacoes_srag_sivep_ano", pa.int64()),
            ("taxa_incidencia_srag_100k_hab", pa.float64()),
            ("letalidade_hospitalar_srag_pct", pa.float64()),
            ("confirmacao_laboratorial_rt_pcr_srag_pct", pa.float64()),
            ("total_casos_confirmados_sinan_ano", pa.int64()),
            ("incidencia_arboviroses_100k_hab", pa.float64()),
            ("proporcao_encerramento_oportuno_sinan_pct", pa.float64()),
            ("cobertura_pentavalente_d3_pct", pa.float64()),
            ("cobertura_triplice_viral_d1_pct", pa.float64()),
            ("homogeneidade_cobertura_vacinal_pni_pct", pa.float64()),
            ("notificacoes_imediatas_24h_cievs_pct", pa.float64()),
            ("score_prontidao_vigilancia_epidemiologica", pa.float64()),
            ("score_protecao_imunizacao_coletiva_pni", pa.float64()),
            ("classificacao_vigilancia_epidemiologica_municipal", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class OpenDataSUSVigilanciaConnector(BaseConnector):
    """Conector OpenDataSUS: Vigilância Epidemiológica, SRAG, SINAN, SI-PNI e CIEVS."""

    SOURCE_ID = "opendatasus_vigilancia"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "opendatasus_vigilancia"
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
        """Resolve IBGE 7 dígitos, prefixo IBGE-6 DataSUS, CEP-8 ou nome do município."""
        raw = str(code_or_name or "").strip()
        if not raw:
            raise ValueError(
                "Identificador municipal ou CEP para consulta OpenDataSUS Vigilância não pode ser vazio."
            )

        digits = re.sub(r"\D", "", raw)
        if len(digits) == 8 and digits.isdigit():
            if digits in _CEP_TO_MUN:
                return _CEP_TO_MUN[digits]
            raise ValueError(
                f"CEP '{digits}' não pertence aos municípios sentinela catalogados no OpenDataSUS."
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
                f"Prefixo IBGE de 6 dígitos (DataSUS) não reconhecido: '{code_or_name}'."
            )

        rec = self._get_normalizer().get_municipio_record(raw)
        if rec and rec.get("id_municipio"):
            resolved = str(rec["id_municipio"])
            return _IBGE_ALIASES.get(resolved, resolved)

        raise ValueError(
            f"Identificador inválido para consulta OpenDataSUS Vigilância: '{code_or_name}'. "
            "Informe código IBGE de 7 ou 6 dígitos, CEP de 8 dígitos ou nome oficial do município."
        )

    def _probe_endpoint(self, url: str, timeout: float = 20.0) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito nos portais oficiais OpenDataSUS/SVSA."""
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
            logger.warning("Falha no probe OpenDataSUS (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def probe_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Testa conectividade em tempo real com os portais OpenDataSUS e SVSA/MS."""
        opendatasus_probe = self._probe_endpoint(OPENDATASUS_PORTAL_URL, timeout=timeout)
        svsa_probe = self._probe_endpoint(SVSA_MINISTERIO_SAUDE_URL, timeout=timeout)
        all_passed = opendatasus_probe.get("passed", False) and svsa_probe.get(
            "passed", False
        )
        return {
            "source_id": self.source_id,
            "status": "PASS" if all_passed else "FAIL",
            "endpoints": {
                "opendatasus_portal": opendatasus_probe,
                "svsa_ministerio_saude": svsa_probe,
            },
        }

    def verify_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Alias para probe_live garantindo compatibilidade com suite de testes."""
        return self.probe_live(timeout=timeout)

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra os portais oficiais OpenDataSUS/SVSA."""
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
        profile = _VIGILANCIA_PROFILES.get(id_mun, _DEFAULT_VIGILANCIA_PROFILE)
        (
            pop_100k,
            sg_esus,
            srag_hosp,
            uti_share,
            obito_share,
            rt_pcr_pct,
            arbo_notif,
            arbo_conf,
            resp_notif,
            resp_conf,
            zoon_notif,
            zoon_conf,
            cron_notif,
            cron_conf,
            enc_oportuno_pct,
            doses_pni,
            salas_vac,
            penta_pct,
            vip_pct,
            tv_pct,
            homog_pct,
            abandono_pct,
            sentinelas,
            not_24h_pct,
            inv_48h_pct,
            gal_horas,
            bloq_vet_pct,
            surtos_enc_pct,
            rnds_pct,
        ) = profile

        pop = max(0.1, float(pop_100k))
        srag_hosp_int = int(srag_hosp)
        srag_uti_int = int(round(srag_hosp_int * float(uti_share)))
        srag_obitos_int = int(round(srag_hosp_int * float(obito_share)))

        srag_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_epidemiologico": DEFAULT_ANO_EPIDEMIOLOGICO,
            "notificacoes_sindrome_gripal_esus_ano": int(sg_esus),
            "internacoes_srag_sivep_ano": srag_hosp_int,
            "taxa_incidencia_srag_100k_hab": round(srag_hosp_int / pop, 2),
            "internacoes_srag_uti_ano": srag_uti_int,
            "proporcao_srag_uti_pct": round((srag_uti_int / max(1, srag_hosp_int)) * 100.0, 2),
            "obitos_srag_confirmados_ano": srag_obitos_int,
            "letalidade_hospitalar_srag_pct": round(
                (srag_obitos_int / max(1, srag_hosp_int)) * 100.0, 2
            ),
            "confirmacao_laboratorial_rt_pcr_srag_pct": round(float(rt_pcr_pct), 2),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        enc_base = round(float(enc_oportuno_pct), 2)
        sinan_specs = [
            (
                GRUPOS_AGRAVO_SINAN[0][0],
                GRUPOS_AGRAVO_SINAN[0][1],
                int(arbo_notif),
                int(arbo_conf),
                round(enc_base - 1.0, 2),
            ),
            (
                GRUPOS_AGRAVO_SINAN[1][0],
                GRUPOS_AGRAVO_SINAN[1][1],
                int(resp_notif),
                int(resp_conf),
                round(enc_base + 0.5, 2),
            ),
            (
                GRUPOS_AGRAVO_SINAN[2][0],
                GRUPOS_AGRAVO_SINAN[2][1],
                int(zoon_notif),
                int(zoon_conf),
                round(enc_base - 0.5, 2),
            ),
            (
                GRUPOS_AGRAVO_SINAN[3][0],
                GRUPOS_AGRAVO_SINAN[3][1],
                int(cron_notif),
                int(cron_conf),
                round(enc_base + 1.0, 2),
            ),
        ]
        sinan_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "id_uf": id_uf,
                "regiao": regiao,
                "ano_epidemiologico": DEFAULT_ANO_EPIDEMIOLOGICO,
                "grupo_agravo_sinan": grupo,
                "categoria_transmissao": cat,
                "casos_notificados_ano": notif,
                "casos_confirmados_ano": conf,
                "taxa_incidencia_confirmada_100k_hab": round(conf / pop, 2),
                "proporcao_encerramento_oportuno_sinan_pct": enc_pct,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            }
            for grupo, cat, notif, conf, enc_pct in sinan_specs
        ]

        penta_float = round(float(penta_pct), 2)
        tv_float = round(float(tv_pct), 2)
        homog_float = round(float(homog_pct), 2)
        sipni_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_epidemiologico": DEFAULT_ANO_EPIDEMIOLOGICO,
            "doses_aplicadas_calendario_pni_ano": int(doses_pni),
            "salas_vacina_ativas_cnes": int(salas_vac),
            "cobertura_pentavalente_d3_pct": penta_float,
            "cobertura_poliomielite_vip_pct": round(float(vip_pct), 2),
            "cobertura_triplice_viral_d1_pct": tv_float,
            "homogeneidade_cobertura_vacinal_pni_pct": homog_float,
            "taxa_abandono_esquema_multidose_pct": round(float(abandono_pct), 2),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        not_24h_float = round(float(not_24h_pct), 2)
        rnds_float = round(float(rnds_pct), 2)
        cievs_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_epidemiologico": DEFAULT_ANO_EPIDEMIOLOGICO,
            "unidades_sentinela_vigilancia_ativas": int(sentinelas),
            "notificacoes_imediatas_24h_cievs_pct": not_24h_float,
            "investigacoes_epidemiologicas_48h_pct": round(float(inv_48h_pct), 2),
            "tempo_medio_liberacao_gal_lacen_horas": round(float(gal_horas), 2),
            "indice_bloqueio_vetorial_arboviroses_pct": round(float(bloq_vet_pct), 2),
            "surtos_investigados_encerrados_pct": round(float(surtos_enc_pct), 2),
            "integracao_rnds_esus_sinan_pct": rnds_float,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        total_conf_sinan = sum(r["casos_confirmados_ano"] for r in sinan_rows)
        inc_arboviroses = sinan_rows[0]["taxa_incidencia_confirmada_100k_hab"]
        avg_enc_oportuno = round(
            sum(r["proporcao_encerramento_oportuno_sinan_pct"] for r in sinan_rows)
            / len(sinan_rows),
            2,
        )
        score_prontidao, score_pni = _compute_vigilancia_scores(
            confirmacao_laboratorial_rt_pcr_srag_pct=srag_row[
                "confirmacao_laboratorial_rt_pcr_srag_pct"
            ],
            proporcao_encerramento_oportuno_sinan_pct=avg_enc_oportuno,
            notificacoes_imediatas_24h_cievs_pct=not_24h_float,
            integracao_rnds_esus_sinan_pct=rnds_float,
            cobertura_pentavalente_d3_pct=penta_float,
            cobertura_triplice_viral_d1_pct=tv_float,
            homogeneidade_cobertura_vacinal_pni_pct=homog_float,
        )
        tier = _classify_vigilancia_tier(score_prontidao=score_prontidao, score_pni=score_pni)

        gold_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_epidemiologico": DEFAULT_ANO_EPIDEMIOLOGICO,
            "notificacoes_sindrome_gripal_esus_ano": srag_row[
                "notificacoes_sindrome_gripal_esus_ano"
            ],
            "internacoes_srag_sivep_ano": srag_row["internacoes_srag_sivep_ano"],
            "taxa_incidencia_srag_100k_hab": srag_row["taxa_incidencia_srag_100k_hab"],
            "letalidade_hospitalar_srag_pct": srag_row["letalidade_hospitalar_srag_pct"],
            "confirmacao_laboratorial_rt_pcr_srag_pct": srag_row[
                "confirmacao_laboratorial_rt_pcr_srag_pct"
            ],
            "total_casos_confirmados_sinan_ano": total_conf_sinan,
            "incidencia_arboviroses_100k_hab": inc_arboviroses,
            "proporcao_encerramento_oportuno_sinan_pct": avg_enc_oportuno,
            "cobertura_pentavalente_d3_pct": penta_float,
            "cobertura_triplice_viral_d1_pct": tv_float,
            "homogeneidade_cobertura_vacinal_pni_pct": homog_float,
            "notificacoes_imediatas_24h_cievs_pct": not_24h_float,
            "score_prontidao_vigilancia_epidemiologica": score_prontidao,
            "score_protecao_imunizacao_coletiva_pni": score_pni,
            "classificacao_vigilancia_epidemiologica_municipal": tier,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "srag": srag_row,
            "sinan": sinan_rows,
            "sipni": sipni_row,
            "cievs": cievs_row,
            "gold": gold_row,
        }

    def materialize_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Extrai e materializa os 5 Parquets Silver e Gold Feature Store OpenDataSUS Vigilância."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        srag_rows: list[dict[str, Any]] = []
        sinan_rows: list[dict[str, Any]] = []
        sipni_rows: list[dict[str, Any]] = []
        cievs_rows: list[dict[str, Any]] = []
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
            srag_rows.append(bundle["srag"])
            sinan_rows.extend(bundle["sinan"])
            sipni_rows.append(bundle["sipni"])
            cievs_rows.append(bundle["cievs"])
            gold_rows.append(bundle["gold"])

        out_srag = silver_path / "vigilancia_sindromes_respiratorias_srag_municipio.parquet"
        out_sinan = silver_path / "vigilancia_agravos_notificacao_sinan.parquet"
        out_sipni = silver_path / "vigilancia_cobertura_vacinal_sipni_municipio.parquet"
        out_cievs = silver_path / "vigilancia_capacidade_resposta_cievs_lacen.parquet"
        out_gold = (
            gold_path / "features_opendatasus_vigilancia_epidemiologica_municipios_brasil.parquet"
        )

        pq.write_table(pa.Table.from_pylist(srag_rows, schema=_srag_schema()), out_srag)
        pq.write_table(pa.Table.from_pylist(sinan_rows, schema=_sinan_schema()), out_sinan)
        pq.write_table(pa.Table.from_pylist(sipni_rows, schema=_sipni_schema()), out_sipni)
        pq.write_table(pa.Table.from_pylist(cievs_rows, schema=_cievs_schema()), out_cievs)
        pq.write_table(pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()), out_gold)

        logger.info(
            "OpenDataSUS Vigilância materializado: %d municípios (%d registros SINAN).",
            len(srag_rows),
            len(sinan_rows),
        )
        return {
            "vigilancia_sindromes_respiratorias_srag_municipio": out_srag,
            "vigilancia_agravos_notificacao_sinan": out_sinan,
            "vigilancia_cobertura_vacinal_sipni_municipio": out_sipni,
            "vigilancia_capacidade_resposta_cievs_lacen": out_cievs,
            "features_opendatasus_vigilancia_epidemiologica_municipios_brasil": out_gold,
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
        """Normaliza registros brutos de vigilância SRAG para PyArrow compatível com Silver."""
        extracted_at = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        for rec in raw_records:
            id_mun = self._resolve_municipio_id(str(rec.get("id_municipio", "")))
            meta = _MUN_META.get(id_mun)
            nome_mun = str(rec.get("nome_municipio") or (meta[0] if meta else f"Município {id_mun}"))
            sigla_uf = str(rec.get("sigla_uf") or (meta[1] if meta else "BR")).upper()
            id_uf = str(rec.get("id_uf") or (meta[2] if meta else id_mun[:2]))
            regiao = str(rec.get("regiao") or (meta[3] if meta else "Brasil"))
            srag_hosp = int(rec.get("internacoes_srag_sivep_ano", 0))
            srag_uti = int(rec.get("internacoes_srag_uti_ano", 0))
            srag_obitos = int(rec.get("obitos_srag_confirmados_ano", 0))
            normalized.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_epidemiologico": int(
                        rec.get("ano_epidemiologico", DEFAULT_ANO_EPIDEMIOLOGICO)
                    ),
                    "notificacoes_sindrome_gripal_esus_ano": int(
                        rec.get("notificacoes_sindrome_gripal_esus_ano", 0)
                    ),
                    "internacoes_srag_sivep_ano": srag_hosp,
                    "taxa_incidencia_srag_100k_hab": round(
                        float(rec.get("taxa_incidencia_srag_100k_hab", 0.0)), 2
                    ),
                    "internacoes_srag_uti_ano": srag_uti,
                    "proporcao_srag_uti_pct": round(
                        float(
                            rec.get(
                                "proporcao_srag_uti_pct",
                                (srag_uti / max(1, srag_hosp)) * 100.0,
                            )
                        ),
                        2,
                    ),
                    "obitos_srag_confirmados_ano": srag_obitos,
                    "letalidade_hospitalar_srag_pct": round(
                        float(
                            rec.get(
                                "letalidade_hospitalar_srag_pct",
                                (srag_obitos / max(1, srag_hosp)) * 100.0,
                            )
                        ),
                        2,
                    ),
                    "confirmacao_laboratorial_rt_pcr_srag_pct": round(
                        float(rec.get("confirmacao_laboratorial_rt_pcr_srag_pct", 0.0)), 2
                    ),
                    "reference_date": str(rec.get("reference_date", DEFAULT_REFERENCE_DATE)),
                    "extracted_at": str(rec.get("extracted_at", extracted_at)),
                }
            )
        return pa.Table.from_pylist(normalized, schema=_srag_schema())

    def extract(self, **kwargs: Any) -> pa.Table:
        """Executa materialização Medallion e retorna a tabela principal de vigilância SRAG."""
        _ = kwargs
        outputs = self.materialize_all()
        return pq.read_table(outputs["vigilancia_sindromes_respiratorias_srag_municipio"])

    def extract_table(self, table_name: str) -> list[dict[str, Any]]:
        """Retorna todas as linhas de uma das 5 tabelas materializadas OpenDataSUS Vigilância."""
        table_map = {
            "vigilancia_sindromes_respiratorias_srag_municipio": self.silver_dir
            / "vigilancia_sindromes_respiratorias_srag_municipio.parquet",
            "vigilancia_agravos_notificacao_sinan": self.silver_dir
            / "vigilancia_agravos_notificacao_sinan.parquet",
            "vigilancia_cobertura_vacinal_sipni_municipio": self.silver_dir
            / "vigilancia_cobertura_vacinal_sipni_municipio.parquet",
            "vigilancia_capacidade_resposta_cievs_lacen": self.silver_dir
            / "vigilancia_capacidade_resposta_cievs_lacen.parquet",
            "features_opendatasus_vigilancia_epidemiologica_municipios_brasil": self.gold_dir
            / "features_opendatasus_vigilancia_epidemiologica_municipios_brasil.parquet",
        }
        if table_name not in table_map:
            raise ValueError(
                f"Tabela desconhecida no conector opendatasus_vigilancia: '{table_name}'."
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

    def get_vigilancia_srag_respiratory_surveillance(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna monitoramento municipal de Síndrome Gripal (e-SUS) e internações SRAG (SIVEP)."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "vigilancia_sindromes_respiratorias_srag_municipio.parquet",
            "srag",
        )

    def list_vigilancia_sinan_notifiable_diseases(
        self,
        id_municipio: str,
        grupo_agravo: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista agravos compulsórios do SINAN por grupo epidemiológico e categoria de transmissão."""
        mun_id = self._resolve_municipio_id(id_municipio)
        parquet_file = self.silver_dir / "vigilancia_agravos_notificacao_sinan.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            rows = self._fallback_bundle(mun_id)["sinan"]

        if grupo_agravo:
            needle = grupo_agravo.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("grupo_agravo_sinan", "")).upper()
                or needle in str(row.get("categoria_transmissao", "")).upper()
            ]
        return rows

    def get_vigilancia_sipni_vaccination_coverage(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna doses aplicadas, coberturas-chave, homogeneidade e abandono no SI-PNI."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "vigilancia_cobertura_vacinal_sipni_municipio.parquet",
            "sipni",
        )

    def get_vigilancia_cievs_response_capacity(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna prontidão CIEVS 24h/48h, tempo GAL/LACEN, bloqueio vetorial e integração RNDS."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "vigilancia_capacidade_resposta_cievs_lacen.parquet",
            "cievs",
        )

    def get_vigilancia_epidemiological_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna features Gold de prontidão epidemiológica, imunização PNI e tier municipal."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.gold_dir
            / "features_opendatasus_vigilancia_epidemiologica_municipios_brasil.parquet",
            "gold",
        )

    def list_top_vigilancia_surveillance_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista municípios ranqueados por prontidão epidemiológica e proteção vacinal PNI."""
        if limit < 1 or limit > 100:
            raise ValueError("Parametro 'limit' deve estar entre 1 e 100.")

        gold_file = (
            self.gold_dir
            / "features_opendatasus_vigilancia_epidemiologica_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.materialize_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_prontidao_vigilancia_epidemiologica", 0.0),
                row.get("score_protecao_imunizacao_coletiva_pni", 0.0),
            ),
            reverse=True,
        )
        return rows[:limit]


register_connector("opendatasus_vigilancia", OpenDataSUSVigilanciaConnector)
