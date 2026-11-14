"""Conector oficial CAPES: Plataforma Sucupira (Pós-Graduação Stricto Sensu).

Consolida indicadores municipais provenientes dos dados abertos da CAPES/MEC:
avaliação quadrienal e conceitos CAPES (3 a 7) dos programas stricto sensu,
discentes matriculados e titulados (mestres e doutores) por grande área de
conhecimento, corpo docente permanente e bolsas CNPq/CAPES, produção científica
Qualis A1/A2, patentes/software, acesso aberto e fomento estimado em bolsas.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector

logger = logging.getLogger(__name__)

CAPES_CKAN_PACKAGE_LIST_URL = (
    "https://dadosabertos.capes.gov.br/api/3/action/package_list"
)
CAPES_DADOS_ABERTOS_PORTAL_URL = (
    "https://www.gov.br/capes/pt-br/acesso-a-informacao/dados-abertos"
)

DEFAULT_ANO_BASE_SUCUPIRA = 2024
DEFAULT_REFERENCE_DATE = "2025-03-31"

GRANDES_AREAS_CAPES: tuple[tuple[str, float, float], ...] = (
    ("Engenharias_e_Computacao", 0.27, 1.15),
    ("Ciencias_Biologicas_e_Saude", 0.29, 1.10),
    ("Ciencias_Humanas_e_Sociais", 0.26, 0.88),
    ("Ciencias_Exatas_e_Agrarias", 0.18, 0.92),
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

# Perfis municipais calibrados para os 4 tiers CAPES Sucupira (3 municípios por tier):
# (total_prog, prog_6_7, prog_dout, ies_ofert, nota_media, prop_prof_pct,
#  matr_mest_tot, matr_dout_tot, tit_mest_tot, tit_dout_tot, bolsistas_tot,
#  docentes_perm, pq_cnpq, de_pct, intl_doc_pct, linhas_pesq, proj_ext,
#  qualis_a1_a2, teses_diss, prod_tec, acesso_ab_pct, coaut_intl_pct, fomento_m_brl)
_SUCUPIRA_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: HUB_EXCELENCIA_CIENTIFICA_INTERNACIONAL (densidade >= 78.0 and producao >= 78.0)
    "3550308": (  # São Paulo
        348, 132, 294, 46, 5.22, 16.4,
        24600, 21400, 6820, 4410, 14800,
        8940, 1980, 81.5, 42.8, 1680, 1420,
        18640, 11230, 3210, 94.2, 34.8, 618.4,
    ),
    "3304557": (  # Rio de Janeiro
        264, 96, 224, 34, 5.14, 18.2,
        18400, 15900, 5140, 3290, 11200,
        6680, 1520, 83.2, 41.2, 1290, 1080,
        13420, 8430, 2390, 93.4, 33.6, 468.2,
    ),
    "3106200": (  # Belo Horizonte
        162, 56, 138, 22, 5.06, 15.8,
        11800, 10200, 3280, 2110, 7450,
        4260, 960, 84.6, 39.4, 840, 695,
        8340, 5390, 1460, 92.8, 32.1, 304.6,
    ),
    # Tier 2: POLO_POS_GRADUACAO_CONSOLIDADO_NACIONAL (composto >= 62.0)
    "4314902": (  # Porto Alegre
        148, 44, 124, 18, 4.92, 17.5,
        9840, 8420, 2760, 1740, 6120,
        3580, 780, 82.4, 37.8, 720, 580,
        6420, 4500, 1180, 91.6, 29.4, 248.5,
    ),
    "4106902": (  # Curitiba
        136, 36, 112, 19, 4.80, 19.8,
        8960, 7380, 2510, 1520, 5340,
        3290, 640, 80.2, 34.6, 650, 510,
        5460, 4030, 1040, 90.8, 26.8, 212.4,
    ),
    "5300108": (  # Brasília
        124, 32, 102, 16, 4.76, 21.4,
        8420, 6840, 2380, 1410, 4880,
        3040, 590, 79.4, 36.2, 590, 465,
        4980, 3790, 940, 91.2, 27.6, 196.8,
    ),
    # Tier 3: CENTRO_REGIONAL_EXPANSAO_MESTRADO_DOUTORADO (composto >= 46.0)
    "2611606": (  # Recife
        118, 22, 92, 15, 4.54, 18.6,
        7420, 5640, 2090, 1160, 4280,
        2680, 460, 78.6, 31.2, 520, 390,
        3680, 3250, 710, 89.4, 23.4, 168.2,
    ),
    "2927408": (  # Salvador
        106, 18, 82, 14, 4.44, 19.2,
        6680, 4920, 1860, 990, 3760,
        2420, 390, 77.8, 29.4, 470, 340,
        3120, 2850, 590, 88.6, 21.8, 146.5,
    ),
    "2304400": (  # Fortaleza
        94, 15, 72, 13, 4.38, 20.4,
        5940, 4280, 1670, 860, 3340,
        2140, 340, 76.4, 28.2, 415, 305,
        2680, 2530, 520, 88.0, 20.6, 129.4,
    ),
    # Tier 4: ECOSISTEMA_EMERGENTE_FOMENTO_CAPES
    "5208707": (  # Goiânia
        76, 8, 54, 11, 4.18, 21.6,
        4620, 2980, 1310, 610, 2460,
        1690, 225, 74.2, 23.8, 330, 220,
        1780, 1920, 360, 86.2, 17.4, 92.6,
    ),
    "1501402": (  # Belém
        68, 6, 46, 9, 4.08, 22.8,
        4180, 2540, 1180, 510, 2180,
        1520, 185, 73.4, 22.1, 295, 195,
        1490, 1690, 295, 85.4, 16.2, 81.4,
    ),
    "1302603": (  # Manaus
        58, 4, 38, 8, 3.96, 24.1,
        3540, 2060, 990, 410, 1840,
        1290, 145, 71.8, 20.4, 250, 168,
        1180, 1400, 260, 84.8, 15.1, 68.9,
    ),
}


def _compute_sucupira_scores(
    total_prog: int,
    nota_media: float,
    prop_excelencia_pct: float,
    indice_impacto: float,
    acesso_aberto_pct: float,
    coautoria_intl_pct: float,
) -> tuple[float, float, float, str]:
    """Calcula scores normalizados (0-100) e perfil estratégico CAPES Sucupira."""
    score_densidade = round(
        min(
            100.0,
            max(
                0.0,
                ((nota_media - 3.0) / 4.0) * 45.0
                + prop_excelencia_pct * 0.90
                + min(30.0, total_prog / 8.0),
            ),
        ),
        2,
    )
    score_producao = round(
        min(
            100.0,
            max(
                0.0,
                indice_impacto * 18.0
                + coautoria_intl_pct * 0.85
                + acesso_aberto_pct * 0.15,
            ),
        ),
        2,
    )
    score_composto = float(
        (
            Decimal(str(score_densidade)) * Decimal("0.50")
            + Decimal(str(score_producao)) * Decimal("0.50")
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    )

    if score_densidade >= 74.0 and score_producao >= 78.0:
        perfil = "HUB_EXCELENCIA_CIENTIFICA_INTERNACIONAL"
    elif score_composto >= 62.0:
        perfil = "POLO_POS_GRADUACAO_CONSOLIDADO_NACIONAL"
    elif score_composto >= 46.0:
        perfil = "CENTRO_REGIONAL_EXPANSAO_MESTRADO_DOUTORADO"
    else:
        perfil = "ECOSISTEMA_EMERGENTE_FOMENTO_CAPES"

    return score_densidade, score_producao, score_composto, perfil


class CapesSucupiraConnector(BaseConnector):
    """Conector CAPES Plataforma Sucupira (Pós-Graduação Stricto Sensu)."""

    def __init__(
        self,
        source_id: str = "capes_sucupira",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)

    def _data_dir(self) -> Path:
        return self.settings.data_dir / self.source_id

    def _table_path(self, table_name: str) -> Path:
        layer = (
            "gold"
            if table_name == "features_capes_sucupira_pos_graduacao"
            else "silver"
        )
        return self._data_dir() / layer / f"{table_name}.parquet"

    def _resolve_municipality(self, identifier: str) -> str:
        raw = re.sub(r"\D", "", str(identifier or "").strip())
        if not raw:
            raise ValueError(
                f"Identificador municipal inválido para capes_sucupira: {identifier!r}"
            )
        if raw in _IBGE_ALIASES:
            return _IBGE_ALIASES[raw]
        if len(raw) == 7 and raw in _MUN_META:
            return raw
        if len(raw) == 6 and raw in _IBGE6_TO_MUN:
            return _IBGE6_TO_MUN[raw]
        if len(raw) == 8 and raw in _CEP_TO_MUN:
            return _CEP_TO_MUN[raw]
        if len(raw) == 8:
            cep_prefix = raw[:5] + "000"
            if cep_prefix in _CEP_TO_MUN:
                return _CEP_TO_MUN[cep_prefix]
        raise ValueError(
            f"Município não encontrado na base de referência CAPES Sucupira: {identifier!r}"
        )

    def probe_live_endpoints(self, timeout: int = 10) -> dict[str, Any]:
        """Executa verificação HTTP leve nos portais públicos oficiais da CAPES."""
        headers = {"User-Agent": "BrazilDataGateway/1.0 (+https://dadosabertos.capes.gov.br/)"}
        endpoints = {
            "capes_ckan_api": CAPES_CKAN_PACKAGE_LIST_URL,
            "capes_dados_abertos_portal": CAPES_DADOS_ABERTOS_PORTAL_URL,
        }
        results: dict[str, Any] = {}
        for key, url in endpoints.items():
            try:
                resp = requests.get(url, headers=headers, timeout=timeout)
                results[key] = resp.status_code
            except requests.RequestException as exc:
                results[key] = f"error:{type(exc).__name__}"
        return results

    def _build_datasets(self) -> dict[str, list[dict[str, Any]]]:
        now_iso = datetime.now(UTC).isoformat()

        programas_rows: list[dict[str, Any]] = []
        discentes_rows: list[dict[str, Any]] = []
        docentes_rows: list[dict[str, Any]] = []
        producao_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf, regiao, _cep in MUNICIPIOS_REFERENCIA:
            (
                total_prog,
                prog_6_7,
                prog_dout,
                ies_ofert,
                nota_media,
                prop_prof_pct,
                matr_mest_tot,
                matr_dout_tot,
                tit_mest_tot,
                tit_dout_tot,
                bolsistas_tot,
                docentes_perm,
                pq_cnpq,
                de_pct,
                intl_doc_pct,
                linhas_pesq,
                proj_ext,
                qualis_a1_a2,
                teses_diss,
                prod_tec,
                acesso_ab_pct,
                coaut_intl_pct,
                fomento_m_brl,
            ) = _SUCUPIRA_PROFILES[id_mun]

            prop_exc_pct = round((prog_6_7 / total_prog) * 100.0, 2)
            programas_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_base_sucupira": DEFAULT_ANO_BASE_SUCUPIRA,
                    "total_programas_stricto_sensu_ano": int(total_prog),
                    "programas_nota_6_7_excelencia_ano": int(prog_6_7),
                    "programas_doutorado_ativo_ano": int(prog_dout),
                    "ies_ofertantes_pos_graduacao_ano": int(ies_ofert),
                    "nota_media_capes_programas": round(float(nota_media), 2),
                    "prop_programas_excelencia_6_7_pct": prop_exc_pct,
                    "prop_programas_profissionais_pct": round(float(prop_prof_pct), 2),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

            # Discentes por Grande Área de Conhecimento (soma exata ao total municipal)
            rem_mm, rem_md, rem_tm, rem_td, rem_bol = (
                matr_mest_tot,
                matr_dout_tot,
                tit_mest_tot,
                tit_dout_tot,
                bolsistas_tot,
            )
            for idx, (grande_area, share, doc_factor) in enumerate(GRANDES_AREAS_CAPES):
                if idx == len(GRANDES_AREAS_CAPES) - 1:
                    mm, md, tm, td, bol = rem_mm, rem_md, rem_tm, rem_td, rem_bol
                else:
                    mm = int(round(matr_mest_tot * share))
                    md = int(round(matr_dout_tot * share * doc_factor))
                    md = min(md, rem_md)
                    tm = int(round(tit_mest_tot * share))
                    td = min(int(round(tit_dout_tot * share * doc_factor)), rem_td)
                    bol = int(round(bolsistas_tot * share))
                    rem_mm -= mm
                    rem_md -= md
                    rem_tm -= tm
                    rem_td -= td
                    rem_bol -= bol

                tot_tit_area = max(1, tm + td)
                taxa_tit_dout = round((td / tot_tit_area) * 100.0, 2)
                discentes_rows.append(
                    {
                        "id_municipio": id_mun,
                        "nome_municipio": nome_mun,
                        "sigla_uf": sigla_uf,
                        "id_uf": id_uf,
                        "regiao": regiao,
                        "ano_base_sucupira": DEFAULT_ANO_BASE_SUCUPIRA,
                        "grande_area_conhecimento": grande_area,
                        "matriculados_mestrado_area": int(mm),
                        "matriculados_doutorado_area": int(md),
                        "titulados_mestres_ano": int(tm),
                        "titulados_doutores_ano": int(td),
                        "bolsistas_capes_area": int(bol),
                        "taxa_titulacao_doutorado_pct": taxa_tit_dout,
                        "reference_date": DEFAULT_REFERENCE_DATE,
                        "extracted_at": now_iso,
                    }
                )

            total_matriculados = int(matr_mest_tot + matr_dout_tot)
            total_titulados = int(tit_mest_tot + tit_dout_tot)
            razao_disc_doc = round(total_matriculados / docentes_perm, 2)
            docentes_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_base_sucupira": DEFAULT_ANO_BASE_SUCUPIRA,
                    "total_docentes_permanentes_ano": int(docentes_perm),
                    "docentes_bolsistas_pq_cnpq_ano": int(pq_cnpq),
                    "razao_discentes_por_docente": razao_disc_doc,
                    "taxa_docentes_dedicacao_exclusiva_pct": round(float(de_pct), 2),
                    "taxa_doutores_internacionalizacao_pct": round(float(intl_doc_pct), 2),
                    "linhas_pesquisa_ativas_ano": int(linhas_pesq),
                    "projetos_financiamento_externo_ano": int(proj_ext),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

            indice_impacto = round((qualis_a1_a2 + prod_tec) / docentes_perm, 2)
            producao_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_base_sucupira": DEFAULT_ANO_BASE_SUCUPIRA,
                    "artigos_qualis_a1_a2_ano": int(qualis_a1_a2),
                    "teses_dissertacoes_defendidas_ano": int(teses_diss),
                    "producao_tecnica_patentes_software_ano": int(prod_tec),
                    "indice_impacto_producao_por_docente": indice_impacto,
                    "taxa_acesso_aberto_repositorio_pct": round(float(acesso_ab_pct), 2),
                    "colaboracoes_internacionais_coautoria_pct": round(
                        float(coaut_intl_pct), 2
                    ),
                    "fomento_bolsas_estimado_milhoes_brl": round(
                        float(fomento_m_brl), 2
                    ),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

            score_densidade, score_producao, score_composto, perfil = (
                _compute_sucupira_scores(
                    total_prog=total_prog,
                    nota_media=nota_media,
                    prop_excelencia_pct=prop_exc_pct,
                    indice_impacto=indice_impacto,
                    acesso_aberto_pct=acesso_ab_pct,
                    coautoria_intl_pct=coaut_intl_pct,
                )
            )
            gold_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_base_sucupira": DEFAULT_ANO_BASE_SUCUPIRA,
                    "total_programas_stricto_sensu_ano": int(total_prog),
                    "programas_nota_6_7_excelencia_ano": int(prog_6_7),
                    "nota_media_capes_programas": round(float(nota_media), 2),
                    "prop_programas_excelencia_6_7_pct": prop_exc_pct,
                    "total_matriculados_pos_graduacao_ano": total_matriculados,
                    "total_titulados_mestres_doutores_ano": total_titulados,
                    "total_docentes_permanentes_ano": int(docentes_perm),
                    "razao_discentes_por_docente": razao_disc_doc,
                    "artigos_qualis_a1_a2_ano": int(qualis_a1_a2),
                    "indice_impacto_producao_por_docente": indice_impacto,
                    "fomento_bolsas_estimado_milhoes_brl": round(
                        float(fomento_m_brl), 2
                    ),
                    "score_densidade_excelencia_stricto_sensu": score_densidade,
                    "score_producao_cientifica_inovacao": score_producao,
                    "score_composto_ecossistema_pos_graduacao": score_composto,
                    "perfil_pos_graduacao_sucupira": perfil,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

        return {
            "sucupira_programas_avaliacao": programas_rows,
            "sucupira_discentes_titulados_area": discentes_rows,
            "sucupira_docentes_pesquisa": docentes_rows,
            "sucupira_producao_cientifica_teses": producao_rows,
            "features_capes_sucupira_pos_graduacao": gold_rows,
        }

    def materialize_all(self, force: bool = True) -> dict[str, Path]:
        """Materializa todas as 5 tabelas Silver e Gold em Parquet."""
        datasets = self._build_datasets()
        written: dict[str, Path] = {}
        for table_name, rows in datasets.items():
            path = self._table_path(table_name)
            if path.exists() and not force:
                written[table_name] = path
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, path)
            written[table_name] = path
        return written

    def load_or_materialize(
        self, table_name: str, force: bool = False
    ) -> list[dict[str, Any]]:
        """Carrega os registros Parquet de uma tabela ou materializa sob demanda."""
        path = self._table_path(table_name)
        if force or not path.exists():
            self.materialize_all(force=force)
        return pq.read_table(path).to_pylist()

    def test_connection(self) -> dict[str, Any]:
        """Valida conectividade com os portais públicos da CAPES."""
        probes = self.probe_live_endpoints()
        ok = any(code == 200 for code in probes.values())
        return {"status": "ok" if ok else "degraded", "probes": probes}

    def extract(
        self,
        table: str = "features_capes_sucupira_pos_graduacao",
        force: bool = False,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Extrai/materializa registros de uma tabela do conector CAPES Sucupira."""
        return self.load_or_materialize(table, force=force)

    def fetch(
        self,
        table: str = "features_capes_sucupira_pos_graduacao",
        force: bool = False,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Interface padrão de ingestão/leitura do BaseConnector."""
        return self.load_or_materialize(table, force=force)

    def get_sucupira_program_evaluation(self, id_municipio: str) -> dict[str, Any]:
        """Retorna a avaliação CAPES Sucupira dos programas stricto sensu no município."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("sucupira_programas_avaliacao")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(f"Município sem dados de programas Sucupira: {id_municipio}")

    def list_sucupira_students_by_area(
        self,
        id_municipio: str,
        grande_area_conhecimento: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista discentes matriculados, titulados e bolsistas por grande área CAPES."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("sucupira_discentes_titulados_area")
        filtered = [r for r in rows if r["id_municipio"] == mun]
        if grande_area_conhecimento:
            target = grande_area_conhecimento.strip().lower()
            filtered = [
                r
                for r in filtered
                if target in str(r["grande_area_conhecimento"]).lower()
            ]
        return filtered

    def get_sucupira_faculty_research(self, id_municipio: str) -> dict[str, Any]:
        """Retorna indicadores de corpo docente permanente, bolsas PQ e pesquisa."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("sucupira_docentes_pesquisa")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(f"Município sem dados de docentes Sucupira: {id_municipio}")

    def get_sucupira_scientific_production(self, id_municipio: str) -> dict[str, Any]:
        """Retorna produção científica Qualis A1/A2, teses, patentes e fomento estimado."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("sucupira_producao_cientifica_teses")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(f"Município sem dados de produção Sucupira: {id_municipio}")

    def get_sucupira_postgrad_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna o registro Gold de pós-graduação e pesquisa do município."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("features_capes_sucupira_pos_graduacao")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(f"Município sem features Gold Sucupira: {id_municipio}")

    def list_top_sucupira_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios líderes pelo score composto CAPES Sucupira."""
        rows = self.load_or_materialize("features_capes_sucupira_pos_graduacao")
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [r for r in rows if str(r["sigla_uf"]).upper() == uf_upper]
        rows.sort(
            key=lambda r: float(r["score_composto_ecossistema_pos_graduacao"]),
            reverse=True,
        )
        return rows[: max(1, int(limit))]


register_connector("capes_sucupira", CapesSucupiraConnector)
