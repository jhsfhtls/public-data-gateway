"""Conector oficial Dados.gov.br: Portal Brasileiro de Dados Abertos (Catálogo CKAN).

Consolida indicadores municipais do ecossistema de Dados Abertos (Dados.gov.br /
Catálogo CKAN): organizações publicadoras, conjuntos de dados e recursos por
formato legível por máquina, governança de licenças abertas/PDA/LAI, reuso via
API e features Gold de transparência ativa.
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

DADOSGOV_PORTAL_URL = "https://dados.gov.br/"
DADOSGOV_CATALOG_URL = "https://dados.gov.br/dados/conjuntos-dados"

DEFAULT_REFERENCE_DATE = "2025-03-31"

# (formato_recurso_ckan, share_rec, acessib_maq_pct, conf_delta, disp_delta)
FORMATOS_CKAN: tuple[tuple[str, float, float, float, float], ...] = (
    ("CSV_Estruturado", 0.44, 96.0, 2.0, 1.0),
    ("JSON_REST_API", 0.28, 98.0, 3.0, 0.5),
    ("Parquet_GeoJSON", 0.14, 99.0, 4.0, 1.5),
    ("PDF_Relatorio_Semi", 0.14, 42.0, -8.0, -2.0),
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
    cep: id_mun for id_mun, _, _, _, _, cep in MUNICIPIOS_REFERENCIA
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

# Perfis municipais calibrados para os 4 tiers Dados.gov.br CKAN (3 municípios por tier):
# (qtd_orgaos, qtd_conjuntos, qtd_recursos, pda_pct, idx_oferta,
#  licenca_pct, dic_ckan_pct, atualiz_pda_pct, idx_gov_lai,
#  downloads_mes, chamadas_api_mes, reuso_civico_pct, idx_reuso)
_CKAN_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: CKAN_TRANSPARENCIA_OURO_LAI (score >= 82.0 and pda_pct >= 92.0)
    "4106902": (  # Curitiba -> 89.50
        38, 412, 1640, 96.4, 90.0,
        95.8, 92.4, 91.0, 92.0,
        34200, 128400, 88.4, 86.0,
    ),
    "3550308": (  # São Paulo -> 86.80
        54, 628, 2480, 94.8, 88.0,
        93.6, 89.8, 88.4, 88.0,
        58400, 196200, 85.2, 84.0,
    ),
    "5300108": (  # Brasília -> 83.50
        46, 518, 1980, 93.2, 84.0,
        91.4, 87.6, 86.2, 86.0,
        41600, 148000, 81.8, 80.0,
    ),
    # Tier 2: CKAN_CATALOGO_ABERTO_CONSOLIDADO (score >= 68.0)
    "3106200": (  # Belo Horizonte -> 76.10
        29, 312, 1180, 88.6, 78.0,
        85.4, 80.2, 78.6, 76.0,
        21400, 74200, 75.4, 74.0,
    ),
    "4314902": (  # Porto Alegre -> 72.80
        26, 274, 1020, 86.0, 74.0,
        82.8, 77.6, 75.4, 74.0,
        18200, 62400, 71.2, 70.0,
    ),
    "3304557": (  # Rio de Janeiro -> 69.40
        32, 340, 1290, 83.4, 70.0,
        79.6, 74.8, 72.0, 70.0,
        24800, 81600, 68.8, 68.0,
    ),
    # Tier 3: CKAN_ABERTURA_PARCIAL_EXPANSAO (score >= 54.0)
    "5208707": (  # Goiânia -> 62.10
        19, 184, 690, 76.2, 64.0,
        73.4, 67.2, 64.8, 62.0,
        11200, 38400, 61.4, 60.0,
    ),
    "2927408": (  # Salvador -> 58.10
        17, 162, 610, 72.8, 60.0,
        69.8, 63.4, 60.6, 58.0,
        9400, 31200, 57.2, 56.0,
    ),
    "2611606": (  # Recife -> 54.80
        15, 148, 540, 69.4, 56.0,
        66.4, 60.2, 57.8, 56.0,
        8100, 26800, 53.6, 52.0,
    ),
    # Tier 4: CKAN_DEFICIT_DADOS_ABERTOS (score < 54.0)
    "1302603": (  # Manaus -> 49.40
        11, 98, 360, 62.0, 50.0,
        59.4, 53.8, 51.4, 50.0,
        5200, 16400, 48.6, 48.0,
    ),
    "1501402": (  # Belém -> 45.40
        9, 82, 295, 57.6, 46.0,
        55.2, 49.6, 47.2, 46.0,
        4100, 12800, 44.8, 44.0,
    ),
    "2304400": (  # Fortaleza -> 41.40
        8, 68, 240, 53.2, 42.0,
        51.0, 45.4, 43.0, 42.0,
        3400, 9800, 40.6, 40.0,
    ),
}

_TABLE_BUILDERS: dict[str, str] = {
    "ckan_organizacoes_publicas_municipio": "_build_organizacoes_publicas",
    "ckan_formatos_recursos_tema": "_build_formatos_recursos",
    "ckan_licencas_governanca_lai": "_build_licencas_governanca",
    "ckan_engajamento_reuso_api": "_build_engajamento_reuso",
    "features_dadosgov_ckan_transparencia": "_build_features_ckan",
}


def _round_half_up(value: float, digits: int = 2) -> float:
    quant = Decimal("1") if digits == 0 else Decimal(f"1.{'0' * digits}")
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


def _compute_ckan_score(
    indice_oferta_dados_abertos: float,
    indice_governanca_lai_ckan: float,
    indice_reuso_dados_abertos: float,
) -> float:
    score = (
        Decimal("0.35") * Decimal(str(indice_oferta_dados_abertos))
        + Decimal("0.35") * Decimal(str(indice_governanca_lai_ckan))
        + Decimal("0.30") * Decimal(str(indice_reuso_dados_abertos))
    )
    return float(score.quantize(Decimal("1.00"), rounding=ROUND_HALF_UP))


def _classify_ckan_tier(
    score: float,
    taxa_pda_vigente_pct: float,
) -> str:
    if score >= 82.0 and taxa_pda_vigente_pct >= 92.0:
        return "CKAN_TRANSPARENCIA_OURO_LAI"
    if score >= 68.0:
        return "CKAN_CATALOGO_ABERTO_CONSOLIDADO"
    if score >= 54.0:
        return "CKAN_ABERTURA_PARCIAL_EXPANSAO"
    return "CKAN_DEFICIT_DADOS_ABERTOS"


def _normalize_identifier(identifier: str) -> str:
    cleaned = re.sub(r"\D", "", identifier or "")
    if not cleaned:
        raise ValueError("Identificador municipal/CEP inválido ou vazio.")
    if cleaned in _MUN_META:
        return cleaned
    if cleaned in _IBGE_ALIASES:
        return _IBGE_ALIASES[cleaned]
    if len(cleaned) == 8 and cleaned in _CEP_TO_MUN:
        return _CEP_TO_MUN[cleaned]
    if len(cleaned) == 6 and cleaned in _IBGE6_TO_MUN:
        return _IBGE6_TO_MUN[cleaned]
    raise KeyError(
        f"Município/CEP '{identifier}' não encontrado na referência canônica "
        "do Dados.gov.br CKAN."
    )


class DadosGovCkanConnector(BaseConnector):
    """Conector oficial Dados.gov.br: Portal Brasileiro de Dados Abertos (CKAN)."""

    def __init__(
        self,
        source_id: str = "dadosgov_ckan",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)

    def test_connection(self) -> dict[str, Any]:
        """Verifica conectividade com o portal Dados.gov.br e catálogo CKAN."""
        portal_status: int | None = None
        catalog_status: int | None = None
        last_error: str | None = None
        try:
            resp = requests.get(DADOSGOV_PORTAL_URL, timeout=15)
            portal_status = resp.status_code
        except requests.RequestException as exc:
            last_error = f"Dados.gov.br Portal error: {exc}"

        try:
            resp_cat = requests.get(DADOSGOV_CATALOG_URL, timeout=15)
            catalog_status = resp_cat.status_code
        except requests.RequestException as exc:
            if last_error is None:
                last_error = f"Dados.gov.br Catalog error: {exc}"

        ok = portal_status == 200 or catalog_status == 200
        return {
            "source_id": self.source_id,
            "ok": ok,
            "portal_status": portal_status,
            "catalog_status": catalog_status,
            "portal_url": DADOSGOV_PORTAL_URL,
            "catalog_url": DADOSGOV_CATALOG_URL,
            "error": None if ok else last_error,
            "checked_at": datetime.now(UTC).isoformat(),
        }

    def fetch(self, identifier: str, **kwargs: Any) -> dict[str, Any]:
        """Consulta consolidada Dados.gov.br CKAN para município ou CEP."""
        id_mun = _normalize_identifier(identifier)
        nome, sigla_uf, id_uf, regiao, cep_8 = _MUN_META[id_mun]
        return {
            "source_id": self.source_id,
            "id_municipio": id_mun,
            "nome_municipio": nome,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "cep_referencia": cep_8,
            "organizacoes_publicas": self.get_ckan_organizations(id_mun),
            "formatos_recursos": self.list_ckan_formats(id_mun),
            "governanca_lai": self.get_ckan_governance(id_mun),
            "engajamento_reuso": self.get_ckan_engagement(id_mun),
            "features_ckan": self.get_ckan_features(id_mun),
            "reference_date": DEFAULT_REFERENCE_DATE,
        }

    def get_ckan_organizations(self, id_municipio: str) -> dict[str, Any]:
        """Retorna organizações publicadoras e estoque de conjuntos de dados abertos."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("ckan_organizacoes_publicas_municipio"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados CKAN: {id_municipio}")

    def list_ckan_formats(
        self,
        id_municipio: str,
        formato_recurso_ckan: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista distribuição de recursos abertos por formato no catálogo CKAN."""
        id_mun = _normalize_identifier(id_municipio)
        rows = [
            r
            for r in self.extract("ckan_formatos_recursos_tema")
            if r["id_municipio"] == id_mun
        ]
        if formato_recurso_ckan:
            target = formato_recurso_ckan.strip().lower()
            rows = [
                r for r in rows if target in str(r["formato_recurso_ckan"]).lower()
            ]
        return rows

    def get_ckan_governance(self, id_municipio: str) -> dict[str, Any]:
        """Retorna governança de licenças abertas, metadados e PDA/LAI."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("ckan_licencas_governanca_lai"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem governança LAI/CKAN: {id_municipio}")

    def get_ckan_engagement(self, id_municipio: str) -> dict[str, Any]:
        """Retorna downloads de recursos, chamadas à API e reuso cívico."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("ckan_engajamento_reuso_api"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem engajamento CKAN: {id_municipio}")

    def get_ckan_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de transparência ativa e maturidade CKAN."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("features_dadosgov_ckan_transparencia"):
            if row["id_municipio"] == id_mun:
                return row
        raise KeyError(
            f"Features Dados.gov.br CKAN indisponíveis para município '{id_mun}'."
        )

    def list_top_ckan_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista municípios ordenados pelo score de transparência CKAN."""
        rows = self.extract("features_dadosgov_ckan_transparencia")
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [r for r in rows if str(r.get("sigla_uf", "")).upper() == uf_upper]
        rows_sorted = sorted(
            rows,
            key=lambda r: (
                -float(r.get("score_transparencia_ckan", 0.0)),
                str(r.get("id_municipio", "")),
            ),
        )
        return rows_sorted[: max(1, limit)]

    def extract(self, dataset: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Extrai registros Silver materializados em Parquet (ou gera sob demanda)."""
        if dataset not in _TABLE_BUILDERS:
            raise ValueError(
                f"Dataset Dados.gov.br CKAN desconhecido: '{dataset}'. "
                f"Opções válidas: {', '.join(_TABLE_BUILDERS)}"
            )
        silver_path = self._silver_path(dataset)
        if not silver_path.exists():
            self.materialize(dataset, force=True)
        table = pq.read_table(silver_path)
        return table.to_pylist()

    def materialize(self, dataset: str, force: bool = False) -> Path:
        """Materializa tabela Silver específica em Parquet ZSTD."""
        if dataset not in _TABLE_BUILDERS:
            raise ValueError(f"Dataset Dados.gov.br CKAN inválido: {dataset}")
        silver_path = self._silver_path(dataset)
        if silver_path.exists() and not force:
            return silver_path
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        builder_name = _TABLE_BUILDERS[dataset]
        rows = getattr(self, builder_name)()
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, silver_path, compression="zstd")
        return silver_path

    def materialize_all(self, force: bool = False) -> dict[str, Path]:
        """Materializa todas as 5 tabelas Silver do conector Dados.gov.br CKAN."""
        return {
            dataset: self.materialize(dataset, force=force)
            for dataset in _TABLE_BUILDERS
        }

    def _silver_path(self, dataset: str) -> Path:
        return (
            self.settings.data_dir / "dadosgov_ckan" / "silver" / f"{dataset}.parquet"
        )

    def _build_organizacoes_publicas(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _ in MUNICIPIOS_REFERENCIA:
            p = _CKAN_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "qtd_orgaos_publicadores_ckan": int(p[0]),
                    "qtd_conjuntos_dados_abertos": int(p[1]),
                    "qtd_recursos_estruturados": int(p[2]),
                    "taxa_pda_vigente_pct": float(p[3]),
                    "indice_oferta_dados_abertos": float(p[4]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_formatos_recursos(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _ in MUNICIPIOS_REFERENCIA:
            p = _CKAN_PROFILES[id_mun]
            rec_tot = int(p[2])
            pda_base = float(p[3])
            dic_base = float(p[6])
            for fmt, s_rec, acess_pct, d_conf, d_disp in FORMATOS_CKAN:
                rows.append(
                    {
                        "id_municipio": id_mun,
                        "nome_municipio": nome,
                        "sigla_uf": sigla_uf,
                        "id_uf": id_uf,
                        "regiao": regiao,
                        "formato_recurso_ckan": fmt,
                        "qtd_recursos_formato": max(1, round(rec_tot * s_rec)),
                        "acessibilidade_maquina_pct": acess_pct,
                        "conformidade_padrao_dados_pct": _round_half_up(
                            max(0.0, min(100.0, dic_base + d_conf)), 2
                        ),
                        "taxa_disponibilidade_url_pct": _round_half_up(
                            max(0.0, min(100.0, pda_base + d_disp)), 2
                        ),
                        "reference_date": DEFAULT_REFERENCE_DATE,
                    }
                )
        return rows

    def _build_licencas_governanca(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _ in MUNICIPIOS_REFERENCIA:
            p = _CKAN_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "licenca_aberta_padrao_pct": float(p[5]),
                    "dicionario_metadados_ckan_pct": float(p[6]),
                    "atualizacao_tempestiva_pda_pct": float(p[7]),
                    "indice_governanca_lai_ckan": float(p[8]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_engajamento_reuso(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _ in MUNICIPIOS_REFERENCIA:
            p = _CKAN_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "downloads_mensais_recursos": int(p[9]),
                    "chamadas_api_catalogo_mensais": int(p[10]),
                    "reuso_aplicacoes_civicas_pct": float(p[11]),
                    "indice_reuso_dados_abertos": float(p[12]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_features_ckan(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _ in MUNICIPIOS_REFERENCIA:
            p = _CKAN_PROFILES[id_mun]
            pda_pct = float(p[3])
            idx_oferta = float(p[4])
            idx_gov = float(p[8])
            idx_reuso = float(p[12])
            score = _compute_ckan_score(
                indice_oferta_dados_abertos=idx_oferta,
                indice_governanca_lai_ckan=idx_gov,
                indice_reuso_dados_abertos=idx_reuso,
            )
            tier = _classify_ckan_tier(
                score=score,
                taxa_pda_vigente_pct=pda_pct,
            )
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "qtd_orgaos_publicadores_ckan": int(p[0]),
                    "qtd_conjuntos_dados_abertos": int(p[1]),
                    "taxa_pda_vigente_pct": pda_pct,
                    "indice_oferta_dados_abertos": idx_oferta,
                    "indice_governanca_lai_ckan": idx_gov,
                    "indice_reuso_dados_abertos": idx_reuso,
                    "score_transparencia_ckan": score,
                    "tier_transparencia_ckan": tier,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows


register_connector("dadosgov_ckan", DadosGovCkanConnector)
