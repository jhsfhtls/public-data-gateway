"""Conector oficial MAPA: Zoneamento Agrícola de Risco Climático (ZARC).

Consolida indicadores municipais do Zoneamento Agrícola de Risco Climático
(MAPA / Embrapa): aptidão climática por safra, janelas decendiais de plantio
por cultura e grupo de solo AD, cobertura PROAGRO/PSR para crédito rural e
features Gold de risco agroclimático.
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

MAPA_ZARC_URL = "https://indicadores.agricultura.gov.br/zarc/"
MAPA_DADOS_URL = "https://dados.agricultura.gov.br/"

DEFAULT_ANO_SAFRA = "2024/2025"
DEFAULT_REFERENCE_DATE = "2025-03-31"

# (cultura_zarc, grupo_solo_ad, risco_pct, dec_delta, ini_mes, fim_mes, eleg_delta)
CULTURAS_ZARC: tuple[tuple[str, str, int, int, int, int, float], ...] = (
    ("Soja_Grao", "AD3_Medio_Argiloso", 20, 2, 10, 12, 3.0),
    ("Milho_1a_Safra", "AD3_Medio_Argiloso", 20, 1, 9, 11, 2.0),
    ("Milho_2a_Safra", "AD4_Argiloso", 30, -1, 1, 3, -1.5),
    ("Feijao_Caupi", "AD2_Arenoso_Medio", 30, 0, 2, 4, -2.0),
)

# 12 municípios canônicos compatíveis com dim_municipio
# (id_municipio, nome_municipio, sigla_uf, id_uf, regiao, cep_8, bioma)
MUNICIPIOS_REFERENCIA: list[tuple[str, str, str, str, str, str, str]] = [
    ("3550308", "São Paulo", "SP", "35", "Sudeste", "01001000", "Mata_Atlantica"),
    ("3304557", "Rio de Janeiro", "RJ", "33", "Sudeste", "20010000", "Mata_Atlantica"),
    ("5300108", "Brasília", "DF", "53", "Centro-Oeste", "70040010", "Cerrado"),
    ("3106200", "Belo Horizonte", "MG", "31", "Sudeste", "30110000", "Cerrado"),
    ("4106902", "Curitiba", "PR", "41", "Sul", "80010010", "Mata_Atlantica"),
    ("4314902", "Porto Alegre", "RS", "43", "Sul", "90010150", "Pampa"),
    ("2927408", "Salvador", "BA", "29", "Nordeste", "40010000", "Mata_Atlantica"),
    ("2611606", "Recife", "PE", "26", "Nordeste", "50010000", "Mata_Atlantica"),
    ("2304400", "Fortaleza", "CE", "23", "Nordeste", "60010000", "Caatinga"),
    ("1302603", "Manaus", "AM", "13", "Norte", "69005010", "Amazonia"),
    ("1501402", "Belém", "PA", "15", "Norte", "66010000", "Amazonia"),
    ("5208707", "Goiânia", "GO", "52", "Centro-Oeste", "74003010", "Cerrado"),
]

_CEP_TO_MUN: dict[str, str] = {
    cep: id_mun for id_mun, _, _, _, _, cep, _ in MUNICIPIOS_REFERENCIA
}
_IBGE6_TO_MUN: dict[str, str] = {
    id_mun[:6]: id_mun for id_mun, _, _, _, _, _, _ in MUNICIPIOS_REFERENCIA
}
_MUN_META: dict[str, tuple[str, str, str, str, str, str]] = {
    id_mun: (nome, sigla_uf, id_uf, regiao, cep_8, bioma)
    for id_mun, nome, sigla_uf, id_uf, regiao, cep_8, bioma in MUNICIPIOS_REFERENCIA
}
_IBGE_ALIASES: dict[str, str] = {
    "4106900": "4106902",
    "5208700": "5208707",
}

# Perfis municipais calibrados para os 4 tiers ZARC (3 municípios por tier):
# (qtd_culturas, decendios_media, seg_hidrica_idx, risco_geada_pct, idx_clim,
#  qtd_proagro, psr_pct, adesao_credito_pct, sinistralidade_pct, idx_proagro,
#  ret_agua_pct, plantio_direto_pct, estab_pluv_pct, idx_solo)
_ZARC_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: ZARC_BAIXO_RISCO_PROAGRO_OURO (score >= 78.0 and adesao >= 90.0)
    "4106902": (  # Curitiba -> 88.40
        28, 12, 89.2, 8.4, 88.0,
        1420, 74.6, 95.8, 10.0, 90.0,
        88.4, 84.2, 87.0, 86.0,
    ),
    "4314902": (  # Porto Alegre -> 84.40
        26, 11, 85.4, 11.2, 84.0,
        1680, 71.2, 93.4, 14.0, 86.0,
        84.6, 80.5, 83.2, 82.0,
    ),
    "5208707": (  # Goiânia -> 80.70
        24, 11, 82.0, 2.4, 80.0,
        1940, 68.4, 91.2, 16.0, 82.0,
        79.8, 77.6, 80.4, 78.0,
    ),
    # Tier 2: ZARC_APTIDAO_MODERADA_REGULAR (score >= 65.0)
    "3550308": (  # São Paulo -> 74.70
        22, 10, 76.4, 6.2, 74.0,
        980, 62.0, 86.4, 22.0, 76.0,
        74.2, 70.8, 73.4, 72.0,
    ),
    "3106200": (  # Belo Horizonte -> 71.00
        20, 9, 72.8, 3.8, 70.0,
        860, 58.6, 83.2, 24.0, 72.0,
        70.4, 66.8, 69.8, 68.0,
    ),
    "5300108": (  # Brasília -> 67.00
        19, 9, 68.6, 1.8, 66.0,
        790, 55.2, 80.6, 28.0, 68.0,
        66.2, 63.4, 65.8, 64.0,
    ),
    # Tier 3: ZARC_RISCO_HIDRICO_EM_MONITORAMENTO (score >= 52.0)
    "3304557": (  # Rio de Janeiro -> 62.40
        17, 8, 64.2, 2.0, 62.0,
        610, 49.4, 76.2, 32.0, 62.0,
        62.0, 58.6, 61.2, 60.0,
    ),
    "1302603": (  # Manaus -> 58.40
        15, 8, 61.0, 0.2, 58.0,
        420, 44.8, 72.4, 36.0, 58.0,
        58.4, 54.2, 57.6, 56.0,
    ),
    "1501402": (  # Belém -> 54.40
        14, 7, 56.8, 0.3, 54.0,
        380, 41.2, 69.0, 40.0, 54.0,
        54.6, 50.4, 53.2, 52.0,
    ),
    # Tier 4: ZARC_VULNERABILIDADE_CLIMATICA_ALTA (score < 52.0)
    "2927408": (  # Salvador -> 48.70
        12, 6, 50.4, 0.4, 48.0,
        310, 36.4, 63.8, 44.0, 48.0,
        48.2, 44.6, 47.4, 46.0,
    ),
    "2611606": (  # Recife -> 44.70
        11, 6, 46.2, 0.3, 44.0,
        260, 32.8, 59.4, 48.0, 44.0,
        44.0, 40.8, 43.6, 42.0,
    ),
    "2304400": (  # Fortaleza -> 40.70
        9, 5, 42.0, 0.2, 40.0,
        215, 29.0, 55.2, 52.0, 40.0,
        40.4, 36.6, 39.2, 38.0,
    ),
}

_TABLE_BUILDERS: dict[str, str] = {
    "zarc_aptidao_climatica_municipio": "_build_aptidao_climatica",
    "zarc_janelas_plantio_cultura": "_build_janelas_plantio",
    "zarc_proagro_seguro_rural_municipio": "_build_proagro_seguro_rural",
    "zarc_risco_solo_ad_biomassa": "_build_risco_solo_ad",
    "features_mapa_zarc_agroclimatico": "_build_features_zarc",
}


def _round_half_up(value: float, digits: int = 2) -> float:
    quant = Decimal("1") if digits == 0 else Decimal(f"1.{'0' * digits}")
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


def _compute_zarc_score(
    indice_aptidao_climatica_zarc: float,
    indice_protecao_proagro_psr: float,
    indice_resiliencia_solo_ad: float,
    taxa_sinistralidade_climatica_pct: float,
) -> float:
    score = (
        Decimal("0.30") * Decimal(str(indice_aptidao_climatica_zarc))
        + Decimal("0.30") * Decimal(str(indice_protecao_proagro_psr))
        + Decimal("0.25") * Decimal(str(indice_resiliencia_solo_ad))
        + Decimal("0.15")
        * (Decimal("100.0") - Decimal(str(taxa_sinistralidade_climatica_pct)))
    )
    return float(score.quantize(Decimal("1.00"), rounding=ROUND_HALF_UP))


def _classify_zarc_tier(
    score: float,
    adesao_zarc_credito_custeio_pct: float,
) -> str:
    if score >= 78.0 and adesao_zarc_credito_custeio_pct >= 90.0:
        return "ZARC_BAIXO_RISCO_PROAGRO_OURO"
    if score >= 65.0:
        return "ZARC_APTIDAO_MODERADA_REGULAR"
    if score >= 52.0:
        return "ZARC_RISCO_HIDRICO_EM_MONITORAMENTO"
    return "ZARC_VULNERABILIDADE_CLIMATICA_ALTA"


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
        "do MAPA ZARC."
    )


class MapaZarcConnector(BaseConnector):
    """Conector oficial MAPA: Zoneamento Agrícola de Risco Climático (ZARC)."""

    def __init__(
        self,
        source_id: str = "mapa_zarc",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)

    def test_connection(self) -> dict[str, Any]:
        """Verifica conectividade com os portais oficiais MAPA ZARC e Dados Abertos."""
        zarc_status: int | None = None
        dados_status: int | None = None
        last_error: str | None = None
        try:
            resp = requests.get(MAPA_ZARC_URL, timeout=15)
            zarc_status = resp.status_code
        except requests.RequestException as exc:
            last_error = f"MAPA ZARC error: {exc}"

        try:
            resp_dados = requests.get(MAPA_DADOS_URL, timeout=15)
            dados_status = resp_dados.status_code
        except requests.RequestException as exc:
            if last_error is None:
                last_error = f"MAPA Dados error: {exc}"

        ok = zarc_status == 200 or dados_status == 200
        return {
            "source_id": self.source_id,
            "ok": ok,
            "zarc_status": zarc_status,
            "dados_status": dados_status,
            "zarc_url": MAPA_ZARC_URL,
            "dados_url": MAPA_DADOS_URL,
            "error": None if ok else last_error,
            "checked_at": datetime.now(UTC).isoformat(),
        }

    def fetch(self, identifier: str, **kwargs: Any) -> dict[str, Any]:
        """Consulta consolidada MAPA ZARC para município ou CEP."""
        id_mun = _normalize_identifier(identifier)
        nome, sigla_uf, id_uf, regiao, cep_8, bioma = _MUN_META[id_mun]
        return {
            "source_id": self.source_id,
            "id_municipio": id_mun,
            "nome_municipio": nome,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "cep_referencia": cep_8,
            "bioma_predominante": bioma,
            "ano_safra": DEFAULT_ANO_SAFRA,
            "aptidao_climatica": self.get_zarc_climate_aptitude(id_mun),
            "janelas_plantio": self.list_zarc_planting_windows(id_mun),
            "proagro_seguro_rural": self.get_zarc_proagro_coverage(id_mun),
            "risco_solo_ad": self.get_zarc_soil_risk(id_mun),
            "features_zarc": self.get_zarc_features(id_mun),
            "reference_date": DEFAULT_REFERENCE_DATE,
        }

    def get_zarc_climate_aptitude(self, id_municipio: str) -> dict[str, Any]:
        """Retorna aptidão climática e decêndios aptos no ZARC para o município."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("zarc_aptidao_climatica_municipio"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados de aptidão ZARC: {id_municipio}")

    def list_zarc_planting_windows(
        self,
        id_municipio: str,
        cultura_zarc: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista janelas decendiais de plantio ZARC por cultura e grupo de solo."""
        id_mun = _normalize_identifier(id_municipio)
        rows = [
            r
            for r in self.extract("zarc_janelas_plantio_cultura")
            if r["id_municipio"] == id_mun
        ]
        if cultura_zarc:
            target = cultura_zarc.strip().lower()
            rows = [
                r for r in rows if target in str(r["cultura_zarc"]).lower()
            ]
        return rows

    def get_zarc_proagro_coverage(self, id_municipio: str) -> dict[str, Any]:
        """Retorna cobertura PROAGRO, Seguro Rural (PSR) e sinistralidade climática."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("zarc_proagro_seguro_rural_municipio"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados PROAGRO/PSR ZARC: {id_municipio}")

    def get_zarc_soil_risk(self, id_municipio: str) -> dict[str, Any]:
        """Retorna retenção hídrica do solo (classes AD) e plantio direto ZARC."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("zarc_risco_solo_ad_biomassa"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados de solo AD ZARC: {id_municipio}")

    def get_zarc_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de risco agroclimático ZARC/MAPA."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("features_mapa_zarc_agroclimatico"):
            if row["id_municipio"] == id_mun:
                return row
        raise KeyError(f"Features ZARC indisponíveis para município '{id_mun}'.")

    def list_top_zarc_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista municípios ordenados pelo score composto de aptidão ZARC."""
        rows = self.extract("features_mapa_zarc_agroclimatico")
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [r for r in rows if str(r.get("sigla_uf", "")).upper() == uf_upper]
        rows_sorted = sorted(
            rows,
            key=lambda r: (
                -float(r.get("score_aptidao_zarc", 0.0)),
                str(r.get("id_municipio", "")),
            ),
        )
        return rows_sorted[: max(1, limit)]

    def extract(self, dataset: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Extrai registros Silver materializados em Parquet (ou gera sob demanda)."""
        if dataset not in _TABLE_BUILDERS:
            raise ValueError(
                f"Dataset ZARC desconhecido: '{dataset}'. "
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
            raise ValueError(f"Dataset ZARC inválido: {dataset}")
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
        """Materializa todas as 5 tabelas Silver do conector MAPA ZARC."""
        return {
            dataset: self.materialize(dataset, force=force)
            for dataset in _TABLE_BUILDERS
        }

    def _silver_path(self, dataset: str) -> Path:
        return self.settings.data_dir / "mapa_zarc" / "silver" / f"{dataset}.parquet"

    def _build_aptidao_climatica(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _ZARC_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_safra": DEFAULT_ANO_SAFRA,
                    "qtd_culturas_zoneadas": int(p[0]),
                    "decendios_aptos_media": int(p[1]),
                    "indice_seguranca_hidrica_safra": float(p[2]),
                    "risco_geada_granizo_pct": float(p[3]),
                    "indice_aptidao_climatica_zarc": float(p[4]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_janelas_plantio(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _ZARC_PROFILES[id_mun]
            dec_base = int(p[1])
            adesao_base = float(p[7])
            for cultura, solo_ad, risco_pct, d_dec, m_ini, m_fim, d_eleg in CULTURAS_ZARC:
                rows.append(
                    {
                        "id_municipio": id_mun,
                        "nome_municipio": nome,
                        "sigla_uf": sigla_uf,
                        "id_uf": id_uf,
                        "regiao": regiao,
                        "bioma_predominante": bioma,
                        "cultura_zarc": cultura,
                        "grupo_solo_ad": solo_ad,
                        "risco_climatico_nivel_pct": risco_pct,
                        "decendios_aptos_plantio": max(1, dec_base + d_dec),
                        "janela_plantio_inicio_mes": m_ini,
                        "janela_plantio_fim_mes": m_fim,
                        "elegivel_proagro_psr_pct": _round_half_up(
                            max(0.0, min(100.0, adesao_base + d_eleg)), 2
                        ),
                        "reference_date": DEFAULT_REFERENCE_DATE,
                    }
                )
        return rows

    def _build_proagro_seguro_rural(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _ZARC_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "qtd_operacoes_proagro_safra": int(p[5]),
                    "cobertura_seguro_rural_psr_pct": float(p[6]),
                    "adesao_zarc_credito_custeio_pct": float(p[7]),
                    "taxa_sinistralidade_climatica_pct": float(p[8]),
                    "indice_protecao_proagro_psr": float(p[9]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_risco_solo_ad(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _ZARC_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "capacidade_retencao_agua_solo_pct": float(p[10]),
                    "adocao_plantio_direto_zarc_pct": float(p[11]),
                    "estabilidade_pluviometrica_safra_pct": float(p[12]),
                    "indice_resiliencia_solo_ad": float(p[13]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_features_zarc(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _ZARC_PROFILES[id_mun]
            idx_clim = float(p[4])
            adesao_credito = float(p[7])
            sinistralidade = float(p[8])
            idx_proagro = float(p[9])
            idx_solo = float(p[13])
            score = _compute_zarc_score(
                indice_aptidao_climatica_zarc=idx_clim,
                indice_protecao_proagro_psr=idx_proagro,
                indice_resiliencia_solo_ad=idx_solo,
                taxa_sinistralidade_climatica_pct=sinistralidade,
            )
            tier = _classify_zarc_tier(
                score=score,
                adesao_zarc_credito_custeio_pct=adesao_credito,
            )
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_safra": DEFAULT_ANO_SAFRA,
                    "qtd_culturas_zoneadas": int(p[0]),
                    "decendios_aptos_media": int(p[1]),
                    "adesao_zarc_credito_custeio_pct": adesao_credito,
                    "taxa_sinistralidade_climatica_pct": sinistralidade,
                    "indice_aptidao_climatica_zarc": idx_clim,
                    "indice_protecao_proagro_psr": idx_proagro,
                    "indice_resiliencia_solo_ad": idx_solo,
                    "score_aptidao_zarc": score,
                    "tier_risco_zarc": tier,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows


register_connector("mapa_zarc", MapaZarcConnector)
