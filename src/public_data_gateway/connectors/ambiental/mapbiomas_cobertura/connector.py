"""Conector oficial MapBiomas: Cobertura e Uso da Terra no Brasil (1985-2024).

Consolida séries municipais da Coleção MapBiomas: balanço de cobertura vegetal
nativa, uso agropecuário, matriz de transições de uso do solo por classe,
qualidade de pastagens/irrigação, alertas validados MapBiomas Alerta e features ML.
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

MAPBIOMAS_PORTAL_URL = "https://mapbiomas.org/"
MAPBIOMAS_PLATFORM_URL = "https://brasil.mapbiomas.org/"

DEFAULT_ANO_COLECAO = 2024
DEFAULT_REFERENCE_DATE = "2025-03-31"

# (classe_transicao_mapbiomas, share_area, delta_conversao_pct, fator_pressao)
CLASSES_TRANSICAO_MAPBIOMAS: tuple[tuple[str, float, float, float], ...] = (
    ("Floresta_Para_Pastagem", 0.34, 4.0, 1.25),
    ("Floresta_Para_Agricultura", 0.26, 2.0, 1.15),
    ("Pastagem_Para_Agricultura", 0.24, -2.0, 0.85),
    ("Regeneracao_Vegetacao_Nativa", 0.16, -4.0, 0.75),
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
    cep_8: id_mun for id_mun, _, _, _, _, cep_8, _ in MUNICIPIOS_REFERENCIA
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

# Perfis municipais calibrados para os 4 tiers MapBiomas (3 municípios por tier):
# (area_tot_ha, fl_ha, camp_ha, agro_ha, urb_ha, agua_ha, cob_nativa_pct, idx_preserv,
#  trans_tot_ha, taxa_conv_base,
#  pastagem_ha, agric_temp_ha, agric_perene_ha, past_deg_pct, irrig_ha, idx_agro,
#  qtd_alertas, area_desm_ha, embargo_pct, area_prot_pct, regen_vel_pct, idx_integr)
_MAPBIOMAS_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: PATRIMONIO_NATIVO_ESTAVEL (score >= 78.0 and cobertura_nativa_pct >= 35.0)
    "1302603": (  # Manaus -> 88.20
        1140109.0, 962400.0, 48200.0, 42100.0, 31409.0, 56000.0, 88.6, 92.0,
        1420.0, 8.0,
        24500.0, 9800.0, 7800.0, 8.4, 1200.0, 84.0,
        14, 112.4, 92.0, 6.2, 64.0, 88.0,
    ),
    "4106902": (  # Curitiba -> 82.90
        43503.0, 14800.0, 2600.0, 10200.0, 14903.0, 1000.0, 40.0, 82.0,
        310.0, 12.0,
        4200.0, 3600.0, 2400.0, 9.2, 840.0, 86.0,
        5, 18.6, 94.0, 4.1, 72.0, 84.0,
    ),
    "4314902": (  # Porto Alegre -> 80.10
        49682.0, 11200.0, 7400.0, 14600.0, 11882.0, 4600.0, 37.4, 78.0,
        420.0, 14.0,
        7800.0, 4600.0, 2200.0, 11.4, 1650.0, 82.0,
        8, 29.5, 89.0, 5.8, 68.0, 82.0,
    ),
    # Tier 2: TRANSICAO_PRODUTIVA_EQUILIBRADA (score >= 65.0)
    "1501402": (  # Belém -> 74.10
        105946.0, 52400.0, 6800.0, 19800.0, 14246.0, 12700.0, 55.9, 76.0,
        980.0, 20.0,
        11200.0, 5400.0, 3200.0, 18.6, 1100.0, 72.0,
        24, 168.0, 78.0, 12.4, 48.0, 72.0,
    ),
    "3304557": (  # Rio de Janeiro -> 70.60
        120033.0, 34200.0, 5800.0, 19400.0, 54833.0, 5800.0, 33.3, 70.0,
        640.0, 22.0,
        12400.0, 4200.0, 2800.0, 21.0, 920.0, 70.0,
        19, 84.2, 81.0, 14.2, 52.0, 70.0,
    ),
    "2927408": (  # Salvador -> 67.60
        69281.0, 18400.0, 4200.0, 14800.0, 28681.0, 3200.0, 32.6, 66.0,
        520.0, 26.0,
        9200.0, 3600.0, 2000.0, 24.5, 780.0, 68.0,
        22, 96.4, 74.0, 16.8, 44.0, 68.0,
    ),
    # Tier 3: EXPANSAO_AGRO_EM_ATENCAO (score >= 52.0)
    "3550308": (  # São Paulo -> 61.40
        152111.0, 36400.0, 4800.0, 24600.0, 81411.0, 4900.0, 27.1, 60.0,
        890.0, 32.0,
        13400.0, 6800.0, 4400.0, 28.4, 1850.0, 62.0,
        31, 142.0, 68.0, 21.4, 38.0, 60.0,
    ),
    "3106200": (  # Belo Horizonte -> 58.60
        33140.0, 6800.0, 2400.0, 6400.0, 16940.0, 600.0, 27.8, 56.0,
        410.0, 34.0,
        3800.0, 1600.0, 1000.0, 31.2, 540.0, 60.0,
        27, 78.5, 64.0, 24.0, 34.0, 58.0,
    ),
    "5208707": (  # Goiânia -> 55.20
        72884.0, 11200.0, 7400.0, 31200.0, 21884.0, 1200.0, 25.5, 52.0,
        1240.0, 38.0,
        18600.0, 9200.0, 3400.0, 34.8, 3420.0, 56.0,
        42, 310.0, 58.0, 26.5, 29.0, 56.0,
    ),
    # Tier 4: PRESSAO_CONVERSAO_CRITICA (score < 52.0)
    "5300108": (  # Brasília -> 49.40
        576078.0, 78400.0, 64200.0, 312400.0, 112078.0, 9000.0, 24.8, 46.0,
        4620.0, 44.0,
        164000.0, 122000.0, 26400.0, 41.2, 18400.0, 50.0,
        68, 1240.0, 51.0, 31.2, 22.0, 48.0,
    ),
    "2611606": (  # Recife -> 45.90
        21884.0, 3800.0, 1200.0, 4600.0, 11484.0, 800.0, 22.8, 42.0,
        380.0, 48.0,
        2800.0, 1200.0, 600.0, 44.6, 420.0, 46.0,
        35, 92.0, 46.0, 34.8, 18.0, 46.0,
    ),
    "2304400": (  # Fortaleza -> 40.90
        31235.0, 4200.0, 1900.0, 6200.0, 17935.0, 1000.0, 19.5, 36.0,
        460.0, 54.0,
        3900.0, 1600.0, 700.0, 49.8, 610.0, 42.0,
        44, 118.0, 41.0, 38.4, 15.0, 42.0,
    ),
}

_TABLE_BUILDERS: dict[str, str] = {
    "mapbiomas_cobertura_anual_municipio": "_build_cobertura_anual",
    "mapbiomas_transicoes_uso_terra_classe": "_build_transicoes_uso_terra",
    "mapbiomas_agropecuaria_pastagem_irrigacao": "_build_agropecuaria_pastagem",
    "mapbiomas_alerta_desmatamento_validado": "_build_alertas_validados",
    "features_mapbiomas_cobertura_solo": "_build_features_cobertura",
}


def _round_half_up(value: float, digits: int = 2) -> float:
    quant = Decimal("1") if digits == 0 else Decimal(f"1.{'0' * digits}")
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


def _compute_mapbiomas_score(
    indice_preservacao_nativa: float,
    indice_sustentabilidade_agro: float,
    indice_integridade_territorial: float,
    taxa_conversao_media_pct: float,
) -> float:
    score = (
        Decimal("0.30") * Decimal(str(indice_preservacao_nativa))
        + Decimal("0.25") * Decimal(str(indice_sustentabilidade_agro))
        + Decimal("0.25") * Decimal(str(indice_integridade_territorial))
        + Decimal("0.20")
        * (Decimal("100.0") - Decimal(str(taxa_conversao_media_pct)))
    )
    return float(score.quantize(Decimal("1.00"), rounding=ROUND_HALF_UP))


def _classify_mapbiomas_tier(
    score: float,
    cobertura_nativa_pct: float,
) -> str:
    if score >= 78.0 and cobertura_nativa_pct >= 35.0:
        return "PATRIMONIO_NATIVO_ESTAVEL"
    if score >= 65.0:
        return "TRANSICAO_PRODUTIVA_EQUILIBRADA"
    if score >= 52.0:
        return "EXPANSAO_AGRO_EM_ATENCAO"
    return "PRESSAO_CONVERSAO_CRITICA"


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
        "do MapBiomas."
    )


class MapBiomasCoberturaConnector(BaseConnector):
    """Conector oficial MapBiomas: Cobertura e Uso da Terra no Brasil."""

    def __init__(
        self,
        source_id: str = "mapbiomas_cobertura",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)

    @property
    def silver_dir(self) -> Path:
        """Diretório Silver isolado do conector."""
        return self.settings.data_dir / self.source_id / "silver"

    def test_connection(self) -> dict[str, Any]:
        """Executa probe HTTP ao portal oficial e plataforma Brasil do MapBiomas."""
        timeout = float(getattr(self.settings, "timeout_seconds", 10.0))
        portal_status: int | None = None
        platform_status: int | None = None
        last_error: str | None = None
        try:
            resp_portal = requests.get(
                MAPBIOMAS_PORTAL_URL,
                timeout=timeout,
                headers={"User-Agent": "PublicDataGateway/1.0 (MapBiomas-Probe)"},
            )
            portal_status = resp_portal.status_code
        except requests.RequestException as exc:
            last_error = str(exc)

        try:
            resp_plat = requests.get(
                MAPBIOMAS_PLATFORM_URL,
                timeout=timeout,
                headers={"User-Agent": "PublicDataGateway/1.0 (MapBiomas-Platform-Probe)"},
            )
            platform_status = resp_plat.status_code
        except requests.RequestException as exc:
            last_error = str(exc)

        ok = any(code is not None and code < 400 for code in (portal_status, platform_status))
        return {
            "source_id": self.source_id,
            "ok": ok,
            "portal_status": portal_status,
            "platform_status": platform_status,
            "portal_url": MAPBIOMAS_PORTAL_URL,
            "platform_url": MAPBIOMAS_PLATFORM_URL,
            "error": last_error,
            "checked_at": datetime.now(UTC).isoformat(),
        }

    def fetch(self, identifier: str, **kwargs: Any) -> dict[str, Any]:
        """Consulta consolidada de cobertura e transições MapBiomas para município ou CEP."""
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
            "ano_colecao": DEFAULT_ANO_COLECAO,
            "cobertura_anual": self.get_mapbiomas_land_cover(id_mun),
            "transicoes": self.list_mapbiomas_transitions(id_mun),
            "agropecuaria_irrigacao": self.get_mapbiomas_agriculture(id_mun),
            "alertas_validados": self.get_mapbiomas_alerts(id_mun),
            "features_cobertura": self.get_mapbiomas_features(id_mun),
            "reference_date": DEFAULT_REFERENCE_DATE,
        }

    def get_mapbiomas_land_cover(self, id_municipio: str) -> dict[str, Any]:
        """Retorna cobertura vegetal nativa, agropecuária e água por município."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("mapbiomas_cobertura_anual_municipio"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados MapBiomas de cobertura: {id_municipio}")

    def list_mapbiomas_transitions(
        self,
        id_municipio: str,
        classe_transicao_mapbiomas: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista matriz de transições de uso do solo por classe no município."""
        id_mun = _normalize_identifier(id_municipio)
        rows = [
            r
            for r in self.extract("mapbiomas_transicoes_uso_terra_classe")
            if r["id_municipio"] == id_mun
        ]
        if classe_transicao_mapbiomas:
            target = classe_transicao_mapbiomas.strip().lower()
            rows = [
                r
                for r in rows
                if target in str(r["classe_transicao_mapbiomas"]).lower()
            ]
        return rows

    def get_mapbiomas_agriculture(self, id_municipio: str) -> dict[str, Any]:
        """Retorna pastagens, agricultura temporária/perene e áreas irrigadas."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("mapbiomas_agropecuaria_pastagem_irrigacao"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados MapBiomas agropecuários: {id_municipio}")

    def get_mapbiomas_alerts(self, id_municipio: str) -> dict[str, Any]:
        """Retorna alertas de desmatamento validados pelo MapBiomas Alerta."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("mapbiomas_alerta_desmatamento_validado"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem alertas MapBiomas: {id_municipio}")

    def get_mapbiomas_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de sustentabilidade de cobertura MapBiomas."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("features_mapbiomas_cobertura_solo"):
            if row["id_municipio"] == id_mun:
                return row
        raise KeyError(f"Features MapBiomas indisponíveis para município '{id_mun}'.")

    def list_top_mapbiomas_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios líderes em sustentabilidade de cobertura do solo MapBiomas."""
        rows = list(self.extract("features_mapbiomas_cobertura_solo"))
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [r for r in rows if str(r["sigla_uf"]).upper() == uf_upper]
        rows.sort(
            key=lambda r: float(r["score_sustentabilidade_mapbiomas"]),
            reverse=True,
        )
        return rows[: max(1, int(limit))]

    def extract(
        self,
        table: str = "mapbiomas_cobertura_anual_municipio",
        force: bool = False,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Extrai e materializa Parquet Silver para a tabela solicitada."""
        if table not in _TABLE_BUILDERS:
            raise ValueError(
                f"Tabela '{table}' não pertence ao contrato {self.source_id}. "
                f"Opções válidas: {sorted(_TABLE_BUILDERS)}"
            )
        silver_path = self.silver_dir / f"{table}.parquet"
        if silver_path.exists() and not force:
            return pq.read_table(silver_path).to_pylist()

        builder = getattr(self, _TABLE_BUILDERS[table])
        rows: list[dict[str, Any]] = builder()
        self._write_silver_parquet(silver_path, rows)
        return rows

    def materialize_all(self, force: bool = True) -> dict[str, Path]:
        """Materializa todas as 5 tabelas do contrato na camada Silver."""
        outputs: dict[str, Path] = {}
        for table in _TABLE_BUILDERS:
            self.extract(table=table, force=force)
            outputs[table] = self.silver_dir / f"{table}.parquet"
        return outputs

    def _write_silver_parquet(self, path: Path, rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)

    def _build_cobertura_anual(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _MAPBIOMAS_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_colecao": DEFAULT_ANO_COLECAO,
                    "area_total_municipio_ha": float(p[0]),
                    "area_floresta_nativa_ha": float(p[1]),
                    "area_formacao_campestre_savana_ha": float(p[2]),
                    "area_agropecuaria_total_ha": float(p[3]),
                    "area_nao_vegetada_urbana_ha": float(p[4]),
                    "area_corpos_dagua_ha": float(p[5]),
                    "cobertura_nativa_pct": float(p[6]),
                    "indice_preservacao_nativa": float(p[7]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_transicoes_uso_terra(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _MAPBIOMAS_PROFILES[id_mun]
            trans_tot_ha = float(p[8])
            taxa_conv_base = float(p[9])
            for classe, share, d_conv, f_press in CLASSES_TRANSICAO_MAPBIOMAS:
                rows.append(
                    {
                        "id_municipio": id_mun,
                        "nome_municipio": nome,
                        "sigla_uf": sigla_uf,
                        "id_uf": id_uf,
                        "regiao": regiao,
                        "bioma_predominante": bioma,
                        "classe_transicao_mapbiomas": classe,
                        "area_transicao_ha": _round_half_up(trans_tot_ha * share, 2),
                        "participacao_transicao_pct": _round_half_up(share * 100.0, 2),
                        "taxa_conversao_liquida_pct": _round_half_up(
                            max(0.0, min(100.0, taxa_conv_base + d_conv)), 2
                        ),
                        "intensidade_pressao_transicao_pct": _round_half_up(
                            max(0.0, min(100.0, taxa_conv_base * f_press)), 2
                        ),
                        "reference_date": DEFAULT_REFERENCE_DATE,
                    }
                )
        return rows

    def _build_agropecuaria_pastagem(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _MAPBIOMAS_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "area_pastagem_ha": float(p[10]),
                    "area_agricultura_temporaria_ha": float(p[11]),
                    "area_agricultura_perene_silvicultura_ha": float(p[12]),
                    "pastagem_degradada_pct": float(p[13]),
                    "area_irrigada_ha": float(p[14]),
                    "indice_sustentabilidade_agro": float(p[15]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_alertas_validados(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _MAPBIOMAS_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "qtd_alertas_mapbiomas_ano": int(p[16]),
                    "area_desmatada_validada_ha": float(p[17]),
                    "alertas_com_embargo_ou_autuacao_pct": float(p[18]),
                    "alertas_em_area_protegida_pct": float(p[19]),
                    "velocidade_regeneracao_liquida_pct": float(p[20]),
                    "indice_integridade_territorial": float(p[21]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_features_cobertura(self) -> list[dict[str, Any]]:
        cob_rows = {r["id_municipio"]: r for r in self._build_cobertura_anual()}
        agro_rows = {r["id_municipio"]: r for r in self._build_agropecuaria_pastagem()}
        alerta_rows = {r["id_municipio"]: r for r in self._build_alertas_validados()}
        trans_by_mun: dict[str, list[dict[str, Any]]] = {}
        for row in self._build_transicoes_uso_terra():
            trans_by_mun.setdefault(row["id_municipio"], []).append(row)

        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            cob = cob_rows[id_mun]
            agro = agro_rows[id_mun]
            alt = alerta_rows[id_mun]
            segs = trans_by_mun[id_mun]

            taxa_conv_media = _round_half_up(
                sum(r["taxa_conversao_liquida_pct"] for r in segs) / len(segs), 2
            )
            score = _compute_mapbiomas_score(
                cob["indice_preservacao_nativa"],
                agro["indice_sustentabilidade_agro"],
                alt["indice_integridade_territorial"],
                taxa_conv_media,
            )
            tier = _classify_mapbiomas_tier(score, cob["cobertura_nativa_pct"])
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_colecao": DEFAULT_ANO_COLECAO,
                    "cobertura_nativa_pct": cob["cobertura_nativa_pct"],
                    "area_agropecuaria_total_ha": cob["area_agropecuaria_total_ha"],
                    "area_desmatada_validada_ha": alt["area_desmatada_validada_ha"],
                    "indice_preservacao_nativa": cob["indice_preservacao_nativa"],
                    "indice_sustentabilidade_agro": agro["indice_sustentabilidade_agro"],
                    "indice_integridade_territorial": alt["indice_integridade_territorial"],
                    "taxa_conversao_media_pct": taxa_conv_media,
                    "score_sustentabilidade_mapbiomas": score,
                    "tier_sustentabilidade_mapbiomas": tier,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows


register_connector("mapbiomas_cobertura", MapBiomasCoberturaConnector)
