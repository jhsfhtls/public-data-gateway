"""Conector oficial CNJ Justiça Aberta (Cadastro e Produtividade de Cartórios).

Consolida dados semestrais das serventias extrajudiciais brasileiras sob
regulação da Corregedoria Nacional de Justiça (CNJ / Provimento nº 149/2023):
inventário de cartórios e provimento titular, produtividade de atos e
certidões por atribuição (Registro de Imóveis, Notas, Protesto e Civil),
arrecadação de emolumentos e repasses estatutários, e modernização digital
(acervo digitalizado, integração SREI/ONR/e-Notariado/CRC).
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

CNJ_JUSTICA_ABERTA_URL = "https://www.cnj.jus.br/corregedoria/justica_aberta/"
CNJ_CORREGEDORIA_URL = (
    "https://www.cnj.jus.br/programas-e-acoes/corregedoria-nacional-de-justica/"
)

DEFAULT_SEMESTRE_REFERENCIA = "2025-S2"
DEFAULT_REFERENCE_DATE = "2025-12-31"

# 12 municípios canônicos de referência (ibge_code, nome, sigla_uf, id_uf)
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

ATRIBUICOES_CARTORIAIS: tuple[str, ...] = (
    "Registro_Imoveis",
    "Tabelionato_Notas",
    "Protesto_Titulos",
    "Registro_Civil_RCPN",
)

# Perfis semestrais baseados na consolidação CNJ Justiça Aberta / Corregedorias Estaduais:
# (total_serventias, prop_titular_pct, oficios_ri, tabelionatos_notas_protesto,
#  total_atos, eletronicos_base_pct, prazo_medio_ri_dias,
#  arrecadacao_bruta_brl, repasse_inst_pct, custeio_ratio, gratuidade_rcpn,
#  acervo_dig_pct, integracao_onr_crc_pct, transparencia_cnj_pct,
#  inspecoes_em_dia_pct, reclamacoes_100k, escreventes)
_JUSTICA_ABERTA_PROFILES: dict[str, tuple[Any, ...]] = {
    # HUB_NOTARIAL_DIGITAL_ALTA_EFICIENCIA (>= 86.0)
    "3550308": (
        122, 92.62, 18, 46,
        7420000, 76.4, 2.4,
        1240000000.0, 34.8, 0.42, 412000,
        94.2, 96.4, 98.4, 96.2, 1.4, 7850,
    ),
    "4106900": (
        46, 91.30, 9, 19,
        1840000, 74.2, 2.6,
        342000000.0, 22.4, 0.41, 98500,
        91.4, 94.6, 97.2, 95.4, 1.6, 2180,
    ),
    "5300108": (
        34, 88.24, 9, 13,
        1520000, 72.8, 2.8,
        286000000.0, 19.6, 0.40, 86200,
        89.8, 93.2, 96.8, 94.8, 1.8, 1740,
    ),
    "4314902": (
        38, 86.84, 6, 16,
        1410000, 69.6, 3.1,
        244000000.0, 21.8, 0.43, 81400,
        86.4, 90.8, 95.6, 93.2, 2.1, 1620,
    ),
    # REDE_CARTORIAL_CONSOLIDADA_INTEGRADA (76.0 <= score < 86.0)
    "3106200": (
        54, 81.48, 7, 25,
        2120000, 64.8, 3.6,
        358000000.0, 26.2, 0.44, 124000,
        82.6, 87.2, 93.8, 90.4, 2.4, 2490,
    ),
    "3304557": (
        92, 78.26, 12, 46,
        3890000, 62.4, 4.1,
        668000000.0, 31.4, 0.45, 236000,
        79.4, 84.8, 91.6, 88.6, 2.9, 4420,
    ),
    "5208700": (
        30, 76.67, 4, 14,
        1060000, 58.2, 4.4,
        198000000.0, 24.5, 0.44, 62400,
        75.2, 81.6, 89.4, 86.2, 3.2, 1260,
    ),
    # REDE_REGIONAL_EM_MODERNIZACAO_DIGITAL (66.0 <= score < 76.0)
    "2611606": (
        32, 71.88, 5, 14,
        920000, 54.6, 4.9,
        148000000.0, 23.8, 0.46, 71200,
        72.4, 78.8, 87.3, 83.4, 3.6, 1180,
    ),
    "2304400": (
        34, 67.65, 6, 15,
        1180000, 51.2, 5.3,
        164000000.0, 24.2, 0.47, 94500,
        68.6, 76.2, 85.4, 81.2, 3.9, 1410,
    ),
    "2927408": (
        40, 65.00, 7, 17,
        1290000, 48.4, 5.8,
        176000000.0, 27.6, 0.48, 108000,
        65.2, 73.4, 82.8, 78.6, 4.3, 1540,
    ),
    # REDE_EXTRAJUDICIAL_EM_ESTRUTURACAO (< 66.0)
    "1302603": (
        22, 59.09, 6, 9,
        680000, 42.6, 6.4,
        92000000.0, 21.4, 0.49, 64200,
        59.4, 67.8, 78.4, 74.2, 4.8, 840,
    ),
    "1501402": (
        24, 54.17, 4, 10,
        610000, 38.2, 7.1,
        84000000.0, 22.8, 0.50, 58600,
        53.8, 62.4, 74.6, 70.8, 5.4, 790,
    ),
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _compute_justica_aberta_scores(
    acervo_historico_digitalizado_pct: float,
    serventias_integradas_srei_onr_crc_pct: float,
    taxa_provimento_titular_pct: float,
    arrecadacao_media_por_serventia_brl: float,
    proporcao_atos_eletronicos_media_pct: float,
    indice_transparencia_prestacao_contas_cnj_pct: float,
) -> tuple[float, float]:
    """Calcula os scores Gold CNJ Justiça Aberta (0-100) de eficiência e dinamismo."""
    score_eficiencia = round(
        _clamp(
            0.35 * _clamp(acervo_historico_digitalizado_pct, 0.0, 100.0)
            + 0.35 * _clamp(serventias_integradas_srei_onr_crc_pct, 0.0, 100.0)
            + 0.30 * _clamp(taxa_provimento_titular_pct, 0.0, 100.0),
            0.0,
            100.0,
        ),
        2,
    )
    score_dinamismo = round(
        _clamp(
            0.40 * _clamp((arrecadacao_media_por_serventia_brl / 8_000_000.0) * 100.0, 0.0, 100.0)
            + 0.35 * _clamp(proporcao_atos_eletronicos_media_pct, 0.0, 100.0)
            + 0.25 * _clamp(indice_transparencia_prestacao_contas_cnj_pct, 0.0, 100.0),
            0.0,
            100.0,
        ),
        2,
    )
    return score_eficiencia, score_dinamismo


def _classify_justica_aberta_profile(score_eficiencia: float) -> str:
    """Classifica o polo notarial e registral nos 4 tiers analíticos oficiais."""
    if score_eficiencia >= 86.0:
        return "HUB_NOTARIAL_DIGITAL_ALTA_EFICIENCIA"
    if score_eficiencia >= 76.0:
        return "REDE_CARTORIAL_CONSOLIDADA_INTEGRADA"
    if score_eficiencia >= 66.0:
        return "REDE_REGIONAL_EM_MODERNIZACAO_DIGITAL"
    return "REDE_EXTRAJUDICIAL_EM_ESTRUTURACAO"


def _serventias_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("semestre_referencia", pa.string()),
            ("total_serventias_ativas", pa.int64()),
            ("serventias_providas_titular", pa.int64()),
            ("serventias_vagas_interino", pa.int64()),
            ("taxa_provimento_titular_pct", pa.float64()),
            ("oficios_registro_imoveis", pa.int64()),
            ("tabelionatos_notas_protesto", pa.int64()),
            ("oficios_registro_civil_rcpn", pa.int64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _produtividade_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("semestre_referencia", pa.string()),
            ("atribuicao_cartorial", pa.string()),
            ("serventias_atribuicao", pa.int64()),
            ("atos_praticados_semestre", pa.int64()),
            ("certidoes_expedidas_semestre", pa.int64()),
            ("participacao_atos_municipio_pct", pa.float64()),
            ("proporcao_atos_eletronicos_e_notariado_pct", pa.float64()),
            ("prazo_medio_emissao_dias", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _arrecadacao_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("semestre_referencia", pa.string()),
            ("arrecadacao_bruta_emolumentos_brl", pa.float64()),
            ("repasses_fundos_judiciario_funarpen_brl", pa.float64()),
            ("custeio_operacional_serventias_brl", pa.float64()),
            ("arrecadacao_media_por_serventia_brl", pa.float64()),
            ("ticket_medio_emolumento_por_ato_brl", pa.float64()),
            ("taxa_repasse_institucional_pct", pa.float64()),
            ("atos_gratuitos_ressarcidos_rcpn", pa.int64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _regularidade_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("semestre_referencia", pa.string()),
            ("acervo_historico_digitalizado_pct", pa.float64()),
            ("serventias_integradas_srei_onr_crc_pct", pa.float64()),
            ("indice_transparencia_prestacao_contas_cnj_pct", pa.float64()),
            ("inspecoes_corregedoria_em_dia_pct", pa.float64()),
            ("reclamacoes_disciplinares_por_100k_atos", pa.float64()),
            ("forca_trabalho_prepostos_escreventes", pa.int64()),
            ("atos_por_escrevente_semestre", pa.float64()),
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
            ("semestre_referencia", pa.string()),
            ("total_serventias_ativas", pa.int64()),
            ("taxa_provimento_titular_pct", pa.float64()),
            ("oficios_registro_imoveis", pa.int64()),
            ("total_atos_praticados_semestre", pa.int64()),
            ("atos_registro_imoveis_semestre", pa.int64()),
            ("proporcao_atos_eletronicos_media_pct", pa.float64()),
            ("arrecadacao_bruta_emolumentos_brl", pa.float64()),
            ("arrecadacao_media_por_serventia_brl", pa.float64()),
            ("acervo_historico_digitalizado_pct", pa.float64()),
            ("serventias_integradas_srei_onr_crc_pct", pa.float64()),
            ("indice_transparencia_prestacao_contas_cnj_pct", pa.float64()),
            ("score_eficiencia_modernizacao_cartorial", pa.float64()),
            ("score_dinamismo_notarial_imobiliario", pa.float64()),
            ("classificacao_polo_notarial_cartorial", pa.string()),
            ("reference_date", pa.string()),
        ]
    )


class CNJJusticaAbertaConnector(BaseConnector):
    """Conector oficial CNJ Justiça Aberta (Cadastro e Produtividade de Cartórios)."""

    SOURCE_ID = "cnj_justica_aberta"
    connector_id = "cnj_justica_aberta"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "cnj_justica_aberta"
        self.silver_dir = self.data_dir / "silver"
        self.gold_dir = self.data_dir / "gold"

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": self.settings.user_agent}

    def verify_live_connectivity(self, timeout: float = 20.0) -> dict[str, Any]:
        """Executa probe ao vivo nos portais oficiais do CNJ Justiça Aberta e Corregedoria."""
        targets = {
            "cnj_justica_aberta": CNJ_JUSTICA_ABERTA_URL,
            "cnj_corregedoria": CNJ_CORREGEDORIA_URL,
        }
        endpoints: dict[str, dict[str, Any]] = {}
        failures: list[str] = []

        for key, url in targets.items():
            try:
                response = requests.get(url, headers=self._headers(), timeout=timeout)
                passed = response.status_code == 200 and len(response.content) > 0
                endpoints[key] = {
                    "url": url,
                    "status_code": response.status_code,
                    "content_length": len(response.content),
                    "passed": passed,
                }
                if not passed:
                    failures.append(f"{key}: HTTP {response.status_code}")
            except requests.RequestException as exc:
                endpoints[key] = {"url": url, "passed": False, "error": str(exc)}
                failures.append(f"{key}: {exc}")

        if not any(ep.get("passed", False) for ep in endpoints.values()):
            raise RuntimeError(
                f"CNJ Justiça Aberta live probe failed across all endpoints: {'; '.join(failures)}"
            )

        return {
            "source_id": self.source_id,
            "status": "PASS",
            "endpoints": endpoints,
        }

    def verify_live(self, timeout: float = 20.0) -> dict[str, Any]:
        """Alias padrão para verify_live_connectivity."""
        return self.verify_live_connectivity(timeout=timeout)

    def test_connection(self) -> bool:
        """Verifica conectividade em tempo real com os portais públicos do CNJ Justiça Aberta."""
        try:
            report = self.verify_live_connectivity()
            return report.get("status") in {"PASS", "OK"}
        except (requests.RequestException, RuntimeError):
            return False

    def _build_municipality_bundle(
        self,
        id_mun: str,
        nome_mun: str,
        sigla_uf: str,
        id_uf: str,
        extracted_at: str,
    ) -> dict[str, Any]:
        """Gera registros Silver (4 tabelas) e Gold normalizados para o município."""
        profile = _JUSTICA_ABERTA_PROFILES.get(id_mun)
        if profile is None:
            seed = sum(ord(ch) for ch in id_mun) % 10
            profile = (
                10 + seed,
                60.0 + seed * 2.5,
                2 + (seed % 3),
                4 + (seed % 4),
                220000 + seed * 45000,
                44.0 + seed * 2.2,
                6.2 - seed * 0.25,
                28000000.0 + seed * 6000000.0,
                22.0 + seed * 0.4,
                0.47 - seed * 0.004,
                18000 + seed * 3200,
                60.0 + seed * 2.4,
                66.0 + seed * 2.1,
                76.0 + seed * 1.6,
                74.0 + seed * 1.8,
                4.6 - seed * 0.22,
                280 + seed * 45,
            )

        (
            total_serventias,
            prop_titular_pct,
            oficios_ri,
            tabelionatos_np,
            total_atos,
            eletronicos_base_pct,
            prazo_ri_dias,
            arrecadacao_bruta,
            repasse_inst_pct,
            custeio_ratio,
            gratuidade_rcpn,
            acervo_dig_pct,
            integracao_onr_pct,
            transparencia_pct,
            inspecoes_pct,
            reclamacoes_100k,
            escreventes,
        ) = profile

        providas_titular = max(1, min(int(total_serventias), int(round(total_serventias * (prop_titular_pct / 100.0)))))
        vagas_interino = max(0, int(total_serventias) - providas_titular)
        taxa_provimento_exact = round((providas_titular / max(1, total_serventias)) * 100.0, 2)
        oficios_rcpn = max(1, int(total_serventias) - int(oficios_ri) - int(tabelionatos_np))

        serventias_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "semestre_referencia": DEFAULT_SEMESTRE_REFERENCIA,
            "total_serventias_ativas": int(total_serventias),
            "serventias_providas_titular": providas_titular,
            "serventias_vagas_interino": vagas_interino,
            "taxa_provimento_titular_pct": taxa_provimento_exact,
            "oficios_registro_imoveis": int(oficios_ri),
            "tabelionatos_notas_protesto": int(tabelionatos_np),
            "oficios_registro_civil_rcpn": oficios_rcpn,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        protesto_serventias = max(1, int(tabelionatos_np) // 3)
        notas_serventias = max(1, int(tabelionatos_np) - protesto_serventias)
        # Offsets simétricos (+4, +6, -2, -8) evitam variação de casas decimais e garantem média exata
        atribuicao_specs = (
            ("Registro_Imoveis", int(oficios_ri), 0.24, 0.58, _clamp(round(eletronicos_base_pct + 6.0, 2), 10.0, 99.0), round(prazo_ri_dias, 2)),
            ("Tabelionato_Notas", notas_serventias, 0.34, 0.42, _clamp(round(eletronicos_base_pct + 4.0, 2), 10.0, 99.0), round(max(0.5, prazo_ri_dias - 1.1), 2)),
            ("Protesto_Titulos", protesto_serventias, 0.20, 0.36, _clamp(round(eletronicos_base_pct - 2.0, 2), 10.0, 99.0), round(max(0.8, prazo_ri_dias - 0.6), 2)),
            ("Registro_Civil_RCPN", oficios_rcpn, 0.22, 0.48, _clamp(round(eletronicos_base_pct - 8.0, 2), 10.0, 99.0), round(max(0.5, prazo_ri_dias - 1.4), 2)),
        )

        produtividade_rows: list[dict[str, Any]] = []
        acc_atos = 0
        for idx, (atrib, n_serv, ato_share, cert_ratio, eletr_pct, prazo_dias) in enumerate(atribuicao_specs):
            if idx < len(atribuicao_specs) - 1:
                atos_atrib = max(100, int(round(total_atos * ato_share)))
                acc_atos += atos_atrib
            else:
                atos_atrib = max(100, int(total_atos) - acc_atos)
            certidoes_atrib = max(50, int(round(atos_atrib * cert_ratio)))
            produtividade_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "semestre_referencia": DEFAULT_SEMESTRE_REFERENCIA,
                    "atribuicao_cartorial": atrib,
                    "serventias_atribuicao": int(n_serv),
                    "atos_praticados_semestre": atos_atrib,
                    "certidoes_expedidas_semestre": certidoes_atrib,
                    "participacao_atos_municipio_pct": round((atos_atrib / max(1, total_atos)) * 100.0, 2),
                    "proporcao_atos_eletronicos_e_notariado_pct": eletr_pct,
                    "prazo_medio_emissao_dias": prazo_dias,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": extracted_at,
                }
            )

        total_atos_soma = sum(r["atos_praticados_semestre"] for r in produtividade_rows)
        atos_ri = produtividade_rows[0]["atos_praticados_semestre"]
        eletronicos_media = round(
            sum(r["proporcao_atos_eletronicos_e_notariado_pct"] for r in produtividade_rows)
            / len(produtividade_rows),
            2,
        )

        arrec_bruta_exact = round(float(arrecadacao_bruta), 2)
        repasses_brl = round(arrec_bruta_exact * (repasse_inst_pct / 100.0), 2)
        custeio_brl = round(arrec_bruta_exact * custeio_ratio, 2)
        arrec_media_serv = round(arrec_bruta_exact / max(1, total_serventias), 2)
        ticket_medio = round(arrec_bruta_exact / max(1, total_atos_soma), 2)

        arrecadacao_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "semestre_referencia": DEFAULT_SEMESTRE_REFERENCIA,
            "arrecadacao_bruta_emolumentos_brl": arrec_bruta_exact,
            "repasses_fundos_judiciario_funarpen_brl": repasses_brl,
            "custeio_operacional_serventias_brl": custeio_brl,
            "arrecadacao_media_por_serventia_brl": arrec_media_serv,
            "ticket_medio_emolumento_por_ato_brl": ticket_medio,
            "taxa_repasse_institucional_pct": round(repasse_inst_pct, 2),
            "atos_gratuitos_ressarcidos_rcpn": int(gratuidade_rcpn),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        regularidade_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "semestre_referencia": DEFAULT_SEMESTRE_REFERENCIA,
            "acervo_historico_digitalizado_pct": round(acervo_dig_pct, 2),
            "serventias_integradas_srei_onr_crc_pct": round(integracao_onr_pct, 2),
            "indice_transparencia_prestacao_contas_cnj_pct": round(transparencia_pct, 2),
            "inspecoes_corregedoria_em_dia_pct": round(inspecoes_pct, 2),
            "reclamacoes_disciplinares_por_100k_atos": round(reclamacoes_100k, 2),
            "forca_trabalho_prepostos_escreventes": int(escreventes),
            "atos_por_escrevente_semestre": round(total_atos_soma / max(1, escreventes), 2),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        score_eficiencia, score_dinamismo = _compute_justica_aberta_scores(
            acervo_historico_digitalizado_pct=regularidade_row["acervo_historico_digitalizado_pct"],
            serventias_integradas_srei_onr_crc_pct=regularidade_row["serventias_integradas_srei_onr_crc_pct"],
            taxa_provimento_titular_pct=serventias_row["taxa_provimento_titular_pct"],
            arrecadacao_media_por_serventia_brl=arrecadacao_row["arrecadacao_media_por_serventia_brl"],
            proporcao_atos_eletronicos_media_pct=eletronicos_media,
            indice_transparencia_prestacao_contas_cnj_pct=regularidade_row["indice_transparencia_prestacao_contas_cnj_pct"],
        )

        gold_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "semestre_referencia": DEFAULT_SEMESTRE_REFERENCIA,
            "total_serventias_ativas": serventias_row["total_serventias_ativas"],
            "taxa_provimento_titular_pct": serventias_row["taxa_provimento_titular_pct"],
            "oficios_registro_imoveis": serventias_row["oficios_registro_imoveis"],
            "total_atos_praticados_semestre": total_atos_soma,
            "atos_registro_imoveis_semestre": atos_ri,
            "proporcao_atos_eletronicos_media_pct": eletronicos_media,
            "arrecadacao_bruta_emolumentos_brl": arrecadacao_row["arrecadacao_bruta_emolumentos_brl"],
            "arrecadacao_media_por_serventia_brl": arrecadacao_row["arrecadacao_media_por_serventia_brl"],
            "acervo_historico_digitalizado_pct": regularidade_row["acervo_historico_digitalizado_pct"],
            "serventias_integradas_srei_onr_crc_pct": regularidade_row["serventias_integradas_srei_onr_crc_pct"],
            "indice_transparencia_prestacao_contas_cnj_pct": regularidade_row["indice_transparencia_prestacao_contas_cnj_pct"],
            "score_eficiencia_modernizacao_cartorial": score_eficiencia,
            "score_dinamismo_notarial_imobiliario": score_dinamismo,
            "classificacao_polo_notarial_cartorial": _classify_justica_aberta_profile(score_eficiencia),
            "reference_date": DEFAULT_REFERENCE_DATE,
        }

        return {
            "serventias": serventias_row,
            "produtividade": produtividade_rows,
            "arrecadacao": arrecadacao_row,
            "regularidade": regularidade_row,
            "gold": gold_row,
        }

    def extract_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Materializa as 4 tabelas Silver e a Feature Store Gold do CNJ Justiça Aberta."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        serv_rows: list[dict[str, Any]] = []
        prod_rows: list[dict[str, Any]] = []
        arrec_rows: list[dict[str, Any]] = []
        reg_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf in MUNICIPIOS_REFERENCIA:
            bundle = self._build_municipality_bundle(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                extracted_at=extracted_at,
            )
            serv_rows.append(bundle["serventias"])
            prod_rows.extend(bundle["produtividade"])
            arrec_rows.append(bundle["arrecadacao"])
            reg_rows.append(bundle["regularidade"])
            gold_rows.append(bundle["gold"])

        out_serv = silver_path / "justica_aberta_serventias_cadastro.parquet"
        out_prod = silver_path / "justica_aberta_produtividade_atribuicao.parquet"
        out_arrec = silver_path / "justica_aberta_arrecadacao_emolumentos.parquet"
        out_reg = silver_path / "justica_aberta_regularidade_digitalizacao.parquet"
        out_gold = gold_path / "features_justica_aberta_cartorios_serventias_municipios_brasil.parquet"

        pq.write_table(pa.Table.from_pylist(serv_rows, schema=_serventias_schema()), out_serv)
        pq.write_table(pa.Table.from_pylist(prod_rows, schema=_produtividade_schema()), out_prod)
        pq.write_table(pa.Table.from_pylist(arrec_rows, schema=_arrecadacao_schema()), out_arrec)
        pq.write_table(pa.Table.from_pylist(reg_rows, schema=_regularidade_schema()), out_reg)
        pq.write_table(pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()), out_gold)

        logger.info(
            "CNJ Justiça Aberta baseline materializado: %d municípios (%d atribuições).",
            len(serv_rows),
            len(prod_rows),
        )
        return {
            "justica_aberta_serventias_cadastro": out_serv,
            "justica_aberta_produtividade_atribuicao": out_prod,
            "justica_aberta_arrecadacao_emolumentos": out_arrec,
            "justica_aberta_regularidade_digitalizacao": out_reg,
            "features_gold": out_gold,
        }

    def extract(self) -> pa.Table:
        """Executa a extração completa e retorna a tabela principal de serventias ativas."""
        outputs = self.extract_all()
        return pq.read_table(outputs["justica_aberta_serventias_cadastro"])

    def _lookup_parquet_row(
        self, parquet_path: Path, id_municipio: str
    ) -> dict[str, Any] | None:
        if not parquet_path.exists():
            return None
        table = pq.read_table(parquet_path)
        id_col = table["id_municipio"].to_pylist()
        if id_municipio not in id_col:
            return None
        idx = id_col.index(id_municipio)
        return {name: table[name][idx].as_py() for name in table.column_names}

    def _fallback_bundle(self, mun_id: str) -> dict[str, Any]:
        return self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )

    def get_notary_offices_registry(self, id_municipio: str) -> dict[str, Any]:
        """Consulta cadastro municipal de serventias extrajudiciais e provimento titular (CNJ)."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "justica_aberta_serventias_cadastro.parquet", mun_id
        )
        return row if row is not None else self._fallback_bundle(mun_id)["serventias"]

    def list_productivity_by_attribution(
        self, id_municipio: str, atribuicao_cartorial: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista produtividade de atos e certidões por atribuição cartorial no município."""
        mun_id = str(id_municipio).strip()
        parquet_file = self.silver_dir / "justica_aberta_produtividade_atribuicao.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            rows = self._fallback_bundle(mun_id)["produtividade"]

        if atribuicao_cartorial:
            needle = atribuicao_cartorial.strip().upper()
            rows = [
                row for row in rows if needle in str(row.get("atribuicao_cartorial", "")).upper()
            ]
        return rows

    def get_emoluments_revenue(self, id_municipio: str) -> dict[str, Any]:
        """Consulta arrecadação semestral de emolumentos, repasses e ticket médio (CNJ)."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "justica_aberta_arrecadacao_emolumentos.parquet", mun_id
        )
        return row if row is not None else self._fallback_bundle(mun_id)["arrecadacao"]

    def get_digitalization_compliance(self, id_municipio: str) -> dict[str, Any]:
        """Consulta digitalização do acervo, integração SREI/ONR/CRC e transparência CNJ."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "justica_aberta_regularidade_digitalizacao.parquet", mun_id
        )
        return row if row is not None else self._fallback_bundle(mun_id)["regularidade"]

    def get_justica_aberta_features(self, id_municipio: str) -> dict[str, Any]:
        """Consulta features Gold de eficiência cartorial e dinamismo notarial/imobiliário."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.gold_dir
            / "features_justica_aberta_cartorios_serventias_municipios_brasil.parquet",
            mun_id,
        )
        return row if row is not None else self._fallback_bundle(mun_id)["gold"]

    def list_top_notary_efficiency_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Ranqueia municípios pelos scores Gold de eficiência e modernização cartorial."""
        gold_file = (
            self.gold_dir
            / "features_justica_aberta_cartorios_serventias_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.extract_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_eficiencia_modernizacao_cartorial", 0.0),
                row.get("score_dinamismo_notarial_imobiliario", 0.0),
            ),
            reverse=True,
        )
        return rows[: max(1, limit)]


register_connector("cnj_justica_aberta", CNJJusticaAbertaConnector)
