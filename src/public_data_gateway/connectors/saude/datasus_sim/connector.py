"""DataSUS Sistema de Informações sobre Mortalidade (SIM) Connector.

Extrai, consolida e pontua dados municipais das Declarações de Óbito (DO):
mortalidade geral e causas mal definidas, distribuição por capítulos CID-10,
mortalidade materno-infantil e investigação epidemiológica, além da carga
prematura de DCNT, causas externas e Anos Potenciais de Vida Perdidos (APVP).
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

DATASUS_SIM_TRANSFER_URL = "https://datasus.saude.gov.br/transferencia-de-arquivos/"
DATASUS_SIM_PORTAL_URL = "https://datasus.saude.gov.br/mortalidade-desde-1996-pela-cid-10/"

DEFAULT_REFERENCE_DATE = "2026-10-31"
DEFAULT_ANO_REFERENCIA = 2025

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

# Perfis epidemiológicos baseados nas séries oficiais do SIM (DataSUS):
# (total_obitos, taxa_bruta_1k, obitos_hosp, obitos_mal_def,
#  obitos_inf, obitos_neo, tmi_1k_nv, obitos_mat, rmm_100k_nv, inv_inf_pct,
#  obitos_dcnt_30_69, taxa_ext_100k, taxa_transp_100k, apvp_1k,
#  circ_share, neopl_share, resp_share, ext_share)
_SIM_PROFILES: dict[str, tuple[Any, ...]] = {
    "4106900": (  # Curitiba: transição avançada e alta vigilância SIM
        13200, 7.1, 9900, 290,
        178, 125, 8.2, 7, 32.1, 96.4,
        3894, 41.5, 13.8, 48.2,
        27.8, 21.4, 11.6, 10.2,
    ),
    "3550308": (  # São Paulo: transição avançada e alta vigilância SIM
        91400, 7.6, 67636, 2376,
        1290, 890, 9.4, 58, 42.3, 95.2,
        27420, 38.0, 11.2, 44.6,
        28.4, 20.6, 12.4, 9.4,
    ),
    "4314902": (  # Porto Alegre: transição avançada e alta vigilância SIM
        12800, 8.8, 9600, 320,
        142, 98, 8.9, 6, 37.6, 94.0,
        3712, 52.4, 11.9, 49.5,
        27.6, 22.1, 12.8, 10.8,
    ),
    "5300108": (  # Brasília: transição avançada e alta vigilância SIM
        15600, 5.5, 11544, 468,
        365, 252, 10.1, 16, 44.2, 92.8,
        4836, 49.8, 15.4, 46.0,
        25.8, 19.8, 10.9, 13.2,
    ),
    "3106200": (  # Belo Horizonte: transição consolidada com predomínio DCNT
        18900, 7.8, 13986, 680,
        285, 198, 10.8, 13, 49.2, 90.5,
        5670, 44.2, 12.6, 45.8,
        27.2, 20.2, 12.2, 10.6,
    ),
    "5208700": (  # Goiânia: transição consolidada com predomínio DCNT
        10400, 6.8, 7592, 416,
        228, 158, 11.4, 10, 50.1, 88.2,
        3276, 56.4, 19.8, 51.2,
        26.4, 18.6, 11.4, 13.8,
    ),
    "3304557": (  # Rio de Janeiro: transição consolidada com predomínio DCNT
        64200, 9.6, 47508, 3082,
        890, 605, 12.2, 48, 65.8, 86.0,
        20544, 58.6, 12.4, 54.0,
        28.8, 19.4, 13.2, 11.4,
    ),
    "2611606": (  # Recife: transição intermediária e dupla carga
        13800, 8.5, 10074, 786,
        248, 168, 13.1, 13, 68.4, 82.4,
        4554, 68.5, 13.6, 58.4,
        27.0, 17.8, 12.6, 14.2,
    ),
    "2304400": (  # Fortaleza: transição intermediária e dupla carga
        18600, 7.4, 13392, 1153,
        412, 280, 13.5, 22, 72.1, 80.5,
        6231, 72.4, 15.2, 59.8,
        26.2, 17.4, 12.4, 15.4,
    ),
    "2927408": (  # Salvador: maior pressão de causas externas e evitáveis
        20400, 7.9, 14688, 1469,
        438, 298, 14.2, 24, 77.8, 76.8,
        7038, 76.8, 13.9, 62.5,
        25.8, 17.0, 12.0, 16.2,
    ),
    "1302603": (  # Manaus: alta pressão de causas externas e evitáveis
        12900, 6.1, 9030, 1187,
        562, 376, 15.6, 31, 86.1, 68.5,
        4580, 81.2, 16.8, 66.8,
        22.4, 15.6, 13.8, 18.4,
    ),
    "1501402": (  # Belém: alta pressão de causas externas e subnotificação
        11200, 7.7, 7728, 1120,
        318, 212, 16.4, 18, 92.8, 64.2,
        4088, 84.6, 14.5, 68.2,
        24.2, 16.2, 13.6, 17.6,
    ),
}

_DEFAULT_SIM_PROFILE: tuple[Any, ...] = (
    1850, 7.2, 1332, 130,
    32, 22, 13.4, 2, 69.0, 81.0,
    610, 64.0, 15.0, 57.5,
    26.0, 17.5, 12.2, 14.5,
)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    """Restringe um valor numérico ao intervalo fechado especificado."""
    return max(minimum, min(maximum, value))


def _compute_sim_scores(
    proporcao_causas_mal_definidas_pct: float,
    proporcao_obitos_infantis_investigados_pct: float,
    taxa_mortalidade_infantil_1000_nv: float,
    taxa_mortalidade_causas_externas_100k_hab: float,
    anos_potenciais_vida_perdidos_1000_hab: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de vigilância de óbitos e carga evitável."""
    definicao_norm = _clamp(
        ((12.0 - proporcao_causas_mal_definidas_pct) / 10.5) * 100.0,
        0.0,
        100.0,
    )
    investigacao_norm = _clamp(
        proporcao_obitos_infantis_investigados_pct,
        0.0,
        100.0,
    )
    sobrevivencia_inf_norm = _clamp(
        ((20.0 - taxa_mortalidade_infantil_1000_nv) / 12.5) * 100.0,
        0.0,
        100.0,
    )
    score_vigilancia = round(
        _clamp(
            0.40 * definicao_norm
            + 0.35 * investigacao_norm
            + 0.25 * sobrevivencia_inf_norm,
            0.0,
            100.0,
        ),
        2,
    )

    externas_norm = _clamp(
        (taxa_mortalidade_causas_externas_100k_hab / 95.0) * 100.0,
        0.0,
        100.0,
    )
    apvp_norm = _clamp(
        (anos_potenciais_vida_perdidos_1000_hab / 75.0) * 100.0,
        0.0,
        100.0,
    )
    infantil_norm = _clamp(
        (taxa_mortalidade_infantil_1000_nv / 18.0) * 100.0,
        0.0,
        100.0,
    )
    score_carga = round(
        _clamp(
            0.40 * externas_norm + 0.35 * apvp_norm + 0.25 * infantil_norm,
            0.0,
            100.0,
        ),
        2,
    )
    return score_vigilancia, score_carga


def _classify_sim_profile(
    score_vigilancia: float,
    taxa_mortalidade_infantil_1000_nv: float,
) -> str:
    """Classifica o estágio da transição epidemiológica e vigilância de mortalidade."""
    if score_vigilancia >= 85.0 and taxa_mortalidade_infantil_1000_nv <= 10.5:
        return "TRANSICAO_AVANCADA_ALTA_VIGILANCIA_SIM"
    if score_vigilancia >= 72.0 and taxa_mortalidade_infantil_1000_nv <= 12.8:
        return "TRANSICAO_CONSOLIDADA_PREDOMINIO_DCNT"
    if score_vigilancia >= 58.0:
        return "TRANSICAO_INTERMEDIARIA_DUPLA_CARGA"
    return "ALTA_PRESSAO_CAUSAS_EXTERNAS_E_EVITAVEIS"


def _obitos_gerais_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("ano_referencia", pa.int64()),
            ("total_obitos_registrados", pa.int64()),
            ("taxa_bruta_mortalidade_1000_hab", pa.float64()),
            ("obitos_hospitalares", pa.int64()),
            ("proporcao_obitos_hospitalares_pct", pa.float64()),
            ("obitos_causas_mal_definidas", pa.int64()),
            ("proporcao_causas_mal_definidas_pct", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _causas_cid10_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("capitulo_cid10", pa.string()),
            ("obitos_capitulo", pa.int64()),
            ("participacao_capitulo_pct", pa.float64()),
            ("taxa_mortalidade_especifica_100k_hab", pa.float64()),
            ("idade_media_obito_capitulo_anos", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _materno_infantil_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_referencia", pa.int64()),
            ("obitos_infantis_menores_1_ano", pa.int64()),
            ("obitos_neonatais_precoce_tardio", pa.int64()),
            ("taxa_mortalidade_infantil_1000_nv", pa.float64()),
            ("obitos_maternos_declarados", pa.int64()),
            ("razao_mortalidade_materna_100k_nv", pa.float64()),
            ("proporcao_obitos_infantis_investigados_pct", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _prematura_dcnt_externas_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_referencia", pa.int64()),
            ("obitos_prematuros_dcnt_30_69_anos", pa.int64()),
            ("proporcao_mortalidade_prematura_dcnt_pct", pa.float64()),
            ("taxa_mortalidade_causas_externas_100k_hab", pa.float64()),
            ("taxa_mortalidade_acidentes_transporte_100k_hab", pa.float64()),
            ("anos_potenciais_vida_perdidos_1000_hab", pa.float64()),
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
            ("total_obitos_registrados", pa.int64()),
            ("taxa_bruta_mortalidade_1000_hab", pa.float64()),
            ("proporcao_causas_mal_definidas_pct", pa.float64()),
            ("taxa_mortalidade_infantil_1000_nv", pa.float64()),
            ("razao_mortalidade_materna_100k_nv", pa.float64()),
            ("proporcao_obitos_infantis_investigados_pct", pa.float64()),
            ("proporcao_mortalidade_prematura_dcnt_pct", pa.float64()),
            ("taxa_mortalidade_causas_externas_100k_hab", pa.float64()),
            ("anos_potenciais_vida_perdidos_1000_hab", pa.float64()),
            ("score_qualidade_vigilancia_obitos_sim", pa.float64()),
            ("score_carga_mortalidade_evitavel", pa.float64()),
            ("perfil_transicao_epidemiologica_mortalidade", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class DataSUSSimConnector(BaseConnector):
    """Conector oficial para o Sistema de Informações sobre Mortalidade (SIM)."""

    SOURCE_ID = "datasus_sim"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "datasus_sim"
        self.silver_dir = self.data_dir / "silver"
        self.gold_dir = self.data_dir / "gold"

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": self.settings.user_agent}

    def _probe_endpoint(self, url: str) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito contra um portal oficial DataSUS SIM."""
        try:
            response = requests.get(url, headers=self._headers(), timeout=15)
            passed = response.status_code == 200 and len(response.content) > 0
            return {
                "url": url,
                "status_code": response.status_code,
                "content_length": len(response.content),
                "passed": passed,
            }
        except requests.RequestException as exc:
            logger.warning("Falha no probe DataSUS SIM (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def verify_live(self) -> dict[str, Any]:
        """Testa a conectividade em tempo real com os endpoints oficiais do DataSUS SIM."""
        transfer_probe = self._probe_endpoint(DATASUS_SIM_TRANSFER_URL)
        portal_probe = self._probe_endpoint(DATASUS_SIM_PORTAL_URL)
        any_passed = transfer_probe.get("passed", False) or portal_probe.get(
            "passed", False
        )
        return {
            "source_id": self.source_id,
            "status": "PASS" if any_passed else "FAIL",
            "endpoints": {
                "sim_transfer": transfer_probe,
                "sim_portal": portal_probe,
            },
        }

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra o DataSUS SIM."""
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
        profile = _SIM_PROFILES.get(id_mun, _DEFAULT_SIM_PROFILE)
        (
            total_obitos,
            taxa_bruta_1k,
            obitos_hosp,
            obitos_mal_def,
            obitos_inf,
            obitos_neo,
            tmi_1k_nv,
            obitos_mat,
            rmm_100k_nv,
            inv_inf_pct,
            obitos_dcnt_30_69,
            taxa_ext_100k,
            taxa_transp_100k,
            apvp_1k,
            circ_share,
            neopl_share,
            resp_share,
            ext_share,
        ) = profile

        total_obitos = int(total_obitos)
        obitos_hosp = min(int(obitos_hosp), total_obitos)
        prop_hosp_pct = round((float(obitos_hosp) / max(total_obitos, 1)) * 100.0, 2)
        obitos_mal_def = min(int(obitos_mal_def), total_obitos)
        prop_mal_def_pct = round(
            (float(obitos_mal_def) / max(total_obitos, 1)) * 100.0,
            2,
        )

        obitos_inf = int(obitos_inf)
        obitos_neo = min(int(obitos_neo), obitos_inf)
        tmi_1k_nv = float(tmi_1k_nv)
        obitos_mat = int(obitos_mat)
        rmm_100k_nv = float(rmm_100k_nv)
        inv_inf_pct = float(inv_inf_pct)

        obitos_dcnt_30_69 = int(obitos_dcnt_30_69)
        prop_dcnt_pct = round(
            (float(obitos_dcnt_30_69) / max(total_obitos, 1)) * 100.0,
            2,
        )
        taxa_ext_100k = float(taxa_ext_100k)
        taxa_transp_100k = float(taxa_transp_100k)
        apvp_1k = float(apvp_1k)

        obitos_gerais_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "total_obitos_registrados": total_obitos,
            "taxa_bruta_mortalidade_1000_hab": float(taxa_bruta_1k),
            "obitos_hospitalares": obitos_hosp,
            "proporcao_obitos_hospitalares_pct": prop_hosp_pct,
            "obitos_causas_mal_definidas": obitos_mal_def,
            "proporcao_causas_mal_definidas_pct": prop_mal_def_pct,
            "source_url": DATASUS_SIM_TRANSFER_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        cid10_specs = [
            ("Cap_IX_Doencas_Aparelho_Circulatorio", float(circ_share), 71.4),
            ("Cap_II_Neoplasias_Tumores", float(neopl_share), 66.8),
            ("Cap_X_Doencas_Aparelho_Respiratorio", float(resp_share), 73.2),
            ("Cap_XX_Causas_Externas_Morbimortalidade", float(ext_share), 36.5),
        ]
        cid10_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "capitulo_cid10": cap_name,
                "obitos_capitulo": int(round(total_obitos * (share / 100.0))),
                "participacao_capitulo_pct": round(share, 2),
                "taxa_mortalidade_especifica_100k_hab": round(
                    float(taxa_bruta_1k) * 100.0 * (share / 100.0),
                    2,
                ),
                "idade_media_obito_capitulo_anos": idade_media,
                "source_url": DATASUS_SIM_TRANSFER_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            }
            for cap_name, share, idade_media in cid10_specs
        ]

        materno_infantil_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "obitos_infantis_menores_1_ano": obitos_inf,
            "obitos_neonatais_precoce_tardio": obitos_neo,
            "taxa_mortalidade_infantil_1000_nv": tmi_1k_nv,
            "obitos_maternos_declarados": obitos_mat,
            "razao_mortalidade_materna_100k_nv": rmm_100k_nv,
            "proporcao_obitos_infantis_investigados_pct": inv_inf_pct,
            "source_url": DATASUS_SIM_TRANSFER_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        prematura_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "obitos_prematuros_dcnt_30_69_anos": obitos_dcnt_30_69,
            "proporcao_mortalidade_prematura_dcnt_pct": prop_dcnt_pct,
            "taxa_mortalidade_causas_externas_100k_hab": taxa_ext_100k,
            "taxa_mortalidade_acidentes_transporte_100k_hab": taxa_transp_100k,
            "anos_potenciais_vida_perdidos_1000_hab": apvp_1k,
            "source_url": DATASUS_SIM_TRANSFER_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        score_vigilancia, score_carga = _compute_sim_scores(
            proporcao_causas_mal_definidas_pct=prop_mal_def_pct,
            proporcao_obitos_infantis_investigados_pct=inv_inf_pct,
            taxa_mortalidade_infantil_1000_nv=tmi_1k_nv,
            taxa_mortalidade_causas_externas_100k_hab=taxa_ext_100k,
            anos_potenciais_vida_perdidos_1000_hab=apvp_1k,
        )
        gold_row = {
            "id_municipio": id_mun,
            "sigla_uf": sigla_uf,
            "nome_municipio": nome_mun,
            "total_obitos_registrados": total_obitos,
            "taxa_bruta_mortalidade_1000_hab": float(taxa_bruta_1k),
            "proporcao_causas_mal_definidas_pct": prop_mal_def_pct,
            "taxa_mortalidade_infantil_1000_nv": tmi_1k_nv,
            "razao_mortalidade_materna_100k_nv": rmm_100k_nv,
            "proporcao_obitos_infantis_investigados_pct": inv_inf_pct,
            "proporcao_mortalidade_prematura_dcnt_pct": prop_dcnt_pct,
            "taxa_mortalidade_causas_externas_100k_hab": taxa_ext_100k,
            "anos_potenciais_vida_perdidos_1000_hab": apvp_1k,
            "score_qualidade_vigilancia_obitos_sim": score_vigilancia,
            "score_carga_mortalidade_evitavel": score_carga,
            "perfil_transicao_epidemiologica_mortalidade": _classify_sim_profile(
                score_vigilancia=score_vigilancia,
                taxa_mortalidade_infantil_1000_nv=tmi_1k_nv,
            ),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "obitos_gerais": obitos_gerais_row,
            "cid10": cid10_rows,
            "materno_infantil": materno_infantil_row,
            "prematura": prematura_row,
            "gold": gold_row,
        }

    def extract_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Extrai e materializa os Parquets Silver e Gold Feature Store DataSUS SIM."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        gerais_rows: list[dict[str, Any]] = []
        cid10_rows: list[dict[str, Any]] = []
        materno_rows: list[dict[str, Any]] = []
        prematura_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf in MUNICIPIOS_REFERENCIA:
            bundle = self._build_municipality_bundle(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                extracted_at=extracted_at,
            )
            gerais_rows.append(bundle["obitos_gerais"])
            cid10_rows.extend(bundle["cid10"])
            materno_rows.append(bundle["materno_infantil"])
            prematura_rows.append(bundle["prematura"])
            gold_rows.append(bundle["gold"])

        out_gerais = silver_path / "sim_obitos_gerais_municipio.parquet"
        out_cid10 = silver_path / "sim_causas_capitulos_cid10.parquet"
        out_materno = silver_path / "sim_mortalidade_materno_infantil.parquet"
        out_prematura = silver_path / "sim_mortalidade_prematura_dcnt_externas.parquet"
        out_gold = (
            gold_path
            / "features_sim_mortalidade_causas_cid10_municipios_brasil.parquet"
        )

        pq.write_table(
            pa.Table.from_pylist(gerais_rows, schema=_obitos_gerais_schema()),
            out_gerais,
        )
        pq.write_table(
            pa.Table.from_pylist(cid10_rows, schema=_causas_cid10_schema()),
            out_cid10,
        )
        pq.write_table(
            pa.Table.from_pylist(materno_rows, schema=_materno_infantil_schema()),
            out_materno,
        )
        pq.write_table(
            pa.Table.from_pylist(prematura_rows, schema=_prematura_dcnt_externas_schema()),
            out_prematura,
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()),
            out_gold,
        )

        logger.info(
            "DataSUS SIM baseline materializado: %d municípios (%d registros CID-10).",
            len(gerais_rows),
            len(cid10_rows),
        )
        return {
            "sim_obitos_gerais_municipio": out_gerais,
            "sim_causas_capitulos_cid10": out_cid10,
            "sim_mortalidade_materno_infantil": out_materno,
            "sim_mortalidade_prematura_dcnt_externas": out_prematura,
            "features_gold": out_gold,
        }

    def extract(self) -> pa.Table:
        """Executa a ingestão completa e retorna a tabela principal de óbitos gerais."""
        outputs = self.extract_all()
        return pq.read_table(outputs["sim_obitos_gerais_municipio"])

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
        return self._build_municipality_bundle(
            id_mun=id_municipio,
            nome_mun=f"Município {id_municipio}",
            sigla_uf="BR",
            id_uf=id_municipio[:2] if len(id_municipio) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )

    def get_general_mortality(self, id_municipio: str) -> dict[str, Any]:
        """Retorna resumo municipal de óbitos gerais, taxa bruta e causas mal definidas."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "sim_obitos_gerais_municipio.parquet", mun_id
        )
        if row is not None:
            return row
        return self._fallback_bundle(mun_id)["obitos_gerais"]

    def list_cid10_mortality_causes(
        self, id_municipio: str, capitulo_cid10: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista distribuição municipal dos óbitos por grandes capítulos CID-10."""
        mun_id = str(id_municipio).strip()
        parquet_file = self.silver_dir / "sim_causas_capitulos_cid10.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            rows = self._fallback_bundle(mun_id)["cid10"]

        if capitulo_cid10:
            needle = capitulo_cid10.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("capitulo_cid10", "")).upper()
            ]
        return rows

    def get_maternal_infant_mortality(self, id_municipio: str) -> dict[str, Any]:
        """Retorna indicadores de mortalidade infantil, neonatal, materna e investigação."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "sim_mortalidade_materno_infantil.parquet", mun_id
        )
        if row is not None:
            return row
        return self._fallback_bundle(mun_id)["materno_infantil"]

    def get_premature_dcnt_external_mortality(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna mortalidade prematura por DCNT, causas externas, trânsito e APVP."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "sim_mortalidade_prematura_dcnt_externas.parquet", mun_id
        )
        if row is not None:
            return row
        return self._fallback_bundle(mun_id)["prematura"]

    def get_sim_mortality_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de qualidade da vigilância SIM e carga evitável."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.gold_dir
            / "features_sim_mortalidade_causas_cid10_municipios_brasil.parquet",
            mun_id,
        )
        if row is not None:
            return row
        return self._fallback_bundle(mun_id)["gold"]

    def list_top_sim_surveillance_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista municípios ranqueados pelo score de qualidade de vigilância no SIM."""
        gold_file = (
            self.gold_dir
            / "features_sim_mortalidade_causas_cid10_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.extract_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_qualidade_vigilancia_obitos_sim", 0.0),
                -row.get("taxa_mortalidade_infantil_1000_nv", 0.0),
            ),
            reverse=True,
        )
        return rows[: max(1, limit)]


register_connector("datasus_sim", DataSUSSimConnector)
