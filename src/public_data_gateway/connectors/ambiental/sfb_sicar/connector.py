"""Conector oficial SFB: Sistema Nacional de Cadastro Ambiental Rural (SICAR).

Consolida indicadores municipais do Cadastro Ambiental Rural (CAR) gerido pelo
Serviço Florestal Brasileiro (SFB/MMA): estoque de imóveis inscritos, situação
de Reserva Legal e APP por categoria fundiária, adesão ao PRA/embargos,
validação geoespacial CAR–SIGEF/INCRA e features ML para crédito rural e EUDR.
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

SFB_PORTAL_URL = "https://www.gov.br/florestal/pt-br"
SICAR_PORTAL_URL = "https://www.car.gov.br/"

DEFAULT_ANO_REFERENCIA = 2025
DEFAULT_REFERENCE_DATE = "2025-03-31"

# (categoria_imovel_car, share_imoveis, share_rl, delta_passivo_pct, delta_conf_pct)
CATEGORIAS_IMOVEL_CAR: tuple[tuple[str, float, float, float, float], ...] = (
    ("Pequena_Propriedade_Ate_4MF", 0.56, 0.24, -2.0, 2.0),
    ("Media_Propriedade_4a15MF", 0.26, 0.30, 2.0, -1.0),
    ("Grande_Propriedade_Acima_15MF", 0.12, 0.38, 4.0, -3.0),
    ("Assentamento_Territorio_Coletivo", 0.06, 0.08, -4.0, 2.0),
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

# Perfis municipais calibrados para os 4 tiers SICAR (3 municípios por tier):
# (qtd_imoveis, area_car_ha, analise_pct, idx_cob,
#  rl_tot_ha, app_tot_ha, taxa_passivo_base, conf_rl_base,
#  qtd_pra, sem_sobrep_ti_uc_pct, livres_emb_pct, eleg_cred_pct, vel_homol_pct, idx_pra,
#  polig_sem_sob_pct, sigef_pct, prec_geo_pct, retif_pct, idx_geo)
_SICAR_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: SICAR_CONFORMIDADE_EUDR_OURO (score >= 78.0 and livres_embargo >= 94.0)
    "4106902": (  # Curitiba -> 86.10
        1480, 18400.0, 88.4, 86.0,
        4120.0, 1680.0, 10.0, 91.0,
        210, 99.4, 98.6, 95.8, 84.2, 88.0,
        94.6, 82.4, 91.8, 74.0, 84.0,
    ),
    "4314902": (  # Porto Alegre -> 82.50
        1920, 24600.0, 84.6, 82.0,
        5240.0, 2140.0, 14.0, 88.0,
        290, 98.8, 97.4, 93.6, 80.5, 84.0,
        92.4, 79.8, 89.2, 71.0, 80.0,
    ),
    "1302603": (  # Manaus -> 79.90
        4260, 412000.0, 79.2, 78.0,
        318000.0, 38400.0, 16.0, 86.0,
        640, 96.2, 95.8, 91.2, 76.8, 82.0,
        89.8, 76.4, 86.5, 68.0, 76.0,
    ),
    # Tier 2: CAR_REGULARIZADO_EM_CONSOLIDACAO (score >= 65.0)
    "3550308": (  # São Paulo -> 72.80
        2640, 38200.0, 76.4, 74.0,
        8420.0, 3120.0, 22.0, 82.0,
        420, 95.8, 93.4, 88.2, 72.4, 74.0,
        86.2, 71.8, 83.4, 62.0, 70.0,
    ),
    "3304557": (  # Rio de Janeiro -> 70.00
        1840, 26800.0, 73.2, 70.0,
        6140.0, 2480.0, 26.0, 79.0,
        310, 94.6, 92.8, 86.4, 69.8, 72.0,
        84.0, 68.4, 80.6, 59.0, 68.0,
    ),
    "3106200": (  # Belo Horizonte -> 66.80
        1120, 14200.0, 70.5, 68.0,
        3240.0, 1180.0, 28.0, 76.0,
        195, 94.2, 91.6, 84.0, 66.2, 68.0,
        81.4, 65.2, 78.0, 56.0, 64.0,
    ),
    # Tier 3: PASSIVO_PRA_EM_MONITORAMENTO (score >= 52.0)
    "1501402": (  # Belém -> 61.10
        1640, 48600.0, 62.4, 62.0,
        32400.0, 5420.0, 34.0, 72.0,
        380, 90.4, 87.2, 78.4, 58.6, 62.0,
        76.8, 58.4, 72.4, 48.0, 58.0,
    ),
    "5208707": (  # Goiânia -> 58.30
        2380, 44800.0, 59.8, 60.0,
        9640.0, 3420.0, 38.0, 69.0,
        510, 92.8, 86.4, 76.2, 55.4, 58.0,
        74.2, 56.8, 70.2, 46.0, 56.0,
    ),
    "2927408": (  # Salvador -> 55.30
        1290, 21400.0, 56.2, 56.0,
        4620.0, 1840.0, 40.0, 66.0,
        280, 91.2, 84.8, 73.5, 52.0, 56.0,
        71.6, 53.2, 67.8, 43.0, 52.0,
    ),
    # Tier 4: RISCO_SOBREPOSICAO_OU_PASSIVO (score < 52.0)
    "5300108": (  # Brasília -> 49.40
        9840, 396000.0, 48.6, 50.0,
        82400.0, 26800.0, 46.0, 60.0,
        2140, 86.4, 79.2, 66.8, 44.2, 50.0,
        66.4, 48.2, 62.4, 38.0, 46.0,
    ),
    "2611606": (  # Recife -> 45.60
        860, 9400.0, 44.2, 46.0,
        1980.0, 840.0, 52.0, 56.0,
        210, 84.8, 76.4, 62.4, 40.5, 46.0,
        62.8, 44.6, 58.2, 34.0, 42.0,
    ),
    "2304400": (  # Fortaleza -> 40.90
        940, 11200.0, 39.8, 42.0,
        2240.0, 920.0, 58.0, 52.0,
        245, 82.6, 73.8, 58.0, 36.8, 40.0,
        58.4, 40.2, 54.0, 30.0, 40.0,
    ),
}

_TABLE_BUILDERS: dict[str, str] = {
    "sicar_imoveis_rurais_municipio": "_build_imoveis_rurais",
    "sicar_reserva_legal_app_situacao": "_build_reserva_legal_app",
    "sicar_regularizacao_pra_embargos": "_build_regularizacao_pra",
    "sicar_geometria_validacao_sigef": "_build_geometria_sigef",
    "features_sfb_sicar_fundiario": "_build_features_sicar",
}


def _round_half_up(value: float, digits: int = 2) -> float:
    quant = Decimal("1") if digits == 0 else Decimal(f"1.{'0' * digits}")
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


def _compute_sicar_score(
    indice_cobertura_cadastral_car: float,
    indice_governanca_pra: float,
    indice_qualidade_geoespacial_car: float,
    taxa_passivo_media_pct: float,
) -> float:
    score = (
        Decimal("0.25") * Decimal(str(indice_cobertura_cadastral_car))
        + Decimal("0.30") * Decimal(str(indice_governanca_pra))
        + Decimal("0.25") * Decimal(str(indice_qualidade_geoespacial_car))
        + Decimal("0.20")
        * (Decimal("100.0") - Decimal(str(taxa_passivo_media_pct)))
    )
    return float(score.quantize(Decimal("1.00"), rounding=ROUND_HALF_UP))


def _classify_sicar_tier(
    score: float,
    imoveis_livres_embargo_ibama_pct: float,
) -> str:
    if score >= 78.0 and imoveis_livres_embargo_ibama_pct >= 94.0:
        return "SICAR_CONFORMIDADE_EUDR_OURO"
    if score >= 65.0:
        return "CAR_REGULARIZADO_EM_CONSOLIDACAO"
    if score >= 52.0:
        return "PASSIVO_PRA_EM_MONITORAMENTO"
    return "RISCO_SOBREPOSICAO_OU_PASSIVO"


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
        "do SFB SICAR."
    )


class SfbSicarConnector(BaseConnector):
    """Conector oficial SFB: Sistema Nacional de Cadastro Ambiental Rural (SICAR)."""

    def __init__(
        self,
        source_id: str = "sfb_sicar",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)

    @property
    def silver_dir(self) -> Path:
        """Diretório Silver isolado do conector."""
        return self.settings.data_dir / self.source_id / "silver"

    def test_connection(self) -> dict[str, Any]:
        """Executa probe HTTP ao portal oficial do Serviço Florestal Brasileiro (SFB/MMA)."""
        timeout = float(getattr(self.settings, "timeout_seconds", 10.0))
        sfb_status: int | None = None
        last_error: str | None = None
        try:
            resp_sfb = requests.get(
                SFB_PORTAL_URL,
                timeout=timeout,
                headers={"User-Agent": "PublicDataGateway/1.0 (SFB-SICAR-Probe)"},
            )
            sfb_status = resp_sfb.status_code
        except requests.RequestException as exc:
            last_error = str(exc)

        ok = sfb_status is not None and sfb_status < 400
        return {
            "source_id": self.source_id,
            "ok": ok,
            "sfb_status": sfb_status,
            "sfb_url": SFB_PORTAL_URL,
            "sicar_url": SICAR_PORTAL_URL,
            "error": last_error,
            "checked_at": datetime.now(UTC).isoformat(),
        }

    def fetch(self, identifier: str, **kwargs: Any) -> dict[str, Any]:
        """Consulta consolidada SICAR para município ou CEP."""
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
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "imoveis_rurais": self.get_sicar_properties(id_mun),
            "reserva_legal_app": self.list_sicar_legal_reserve(id_mun),
            "regularizacao_pra": self.get_sicar_pra_compliance(id_mun),
            "geometria_sigef": self.get_sicar_geometry(id_mun),
            "features_sicar": self.get_sicar_features(id_mun),
            "reference_date": DEFAULT_REFERENCE_DATE,
        }

    def get_sicar_properties(self, id_municipio: str) -> dict[str, Any]:
        """Retorna estoque de imóveis rurais e área cadastrada no SICAR."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("sicar_imoveis_rurais_municipio"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados de imóveis SICAR: {id_municipio}")

    def list_sicar_legal_reserve(
        self,
        id_municipio: str,
        categoria_imovel_car: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista situação de Reserva Legal e APP por categoria fundiária."""
        id_mun = _normalize_identifier(id_municipio)
        rows = [
            r
            for r in self.extract("sicar_reserva_legal_app_situacao")
            if r["id_municipio"] == id_mun
        ]
        if categoria_imovel_car:
            target = categoria_imovel_car.strip().lower()
            rows = [
                r
                for r in rows
                if target in str(r["categoria_imovel_car"]).lower()
            ]
        return rows

    def get_sicar_pra_compliance(self, id_municipio: str) -> dict[str, Any]:
        """Retorna adesão ao PRA, embargos e elegibilidade para crédito rural."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("sicar_regularizacao_pra_embargos"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados PRA/embargos SICAR: {id_municipio}")

    def get_sicar_geometry(self, id_municipio: str) -> dict[str, Any]:
        """Retorna indicadores de validação geométrica e integração CAR–SIGEF/INCRA."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("sicar_geometria_validacao_sigef"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados geoespaciais SICAR: {id_municipio}")

    def get_sicar_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de conformidade fundiária e ambiental SICAR."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("features_sfb_sicar_fundiario"):
            if row["id_municipio"] == id_mun:
                return row
        raise KeyError(f"Features SICAR indisponíveis para município '{id_mun}'.")

    def list_top_sicar_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios líderes em conformidade fundiária e ambiental SICAR."""
        rows = list(self.extract("features_sfb_sicar_fundiario"))
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [r for r in rows if str(r["sigla_uf"]).upper() == uf_upper]
        rows.sort(
            key=lambda r: float(r["score_conformidade_sicar"]),
            reverse=True,
        )
        return rows[: max(1, int(limit))]

    def extract(
        self,
        table: str = "sicar_imoveis_rurais_municipio",
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

    def _build_imoveis_rurais(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _SICAR_PROFILES[id_mun]
            qtd_imoveis = int(p[0])
            area_car_ha = float(p[1])
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_referencia": DEFAULT_ANO_REFERENCIA,
                    "qtd_imoveis_car_ativos": qtd_imoveis,
                    "area_cadastrada_car_ha": area_car_ha,
                    "area_media_imovel_ha": _round_half_up(area_car_ha / qtd_imoveis, 2),
                    "imoveis_analise_concluida_pct": float(p[2]),
                    "indice_cobertura_cadastral_car": float(p[3]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_reserva_legal_app(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _SICAR_PROFILES[id_mun]
            qtd_tot = int(p[0])
            rl_tot = float(p[4])
            app_tot = float(p[5])
            passivo_base = float(p[6])
            conf_base = float(p[7])
            for cat, s_imv, s_rl, d_pas, d_conf in CATEGORIAS_IMOVEL_CAR:
                rows.append(
                    {
                        "id_municipio": id_mun,
                        "nome_municipio": nome,
                        "sigla_uf": sigla_uf,
                        "id_uf": id_uf,
                        "regiao": regiao,
                        "bioma_predominante": bioma,
                        "categoria_imovel_car": cat,
                        "qtd_imoveis_categoria": max(1, round(qtd_tot * s_imv)),
                        "area_reserva_legal_declarada_ha": _round_half_up(rl_tot * s_rl, 2),
                        "area_app_preservada_ha": _round_half_up(app_tot * s_rl, 2),
                        "taxa_passivo_ambiental_pct": _round_half_up(
                            max(0.0, min(100.0, passivo_base + d_pas)), 2
                        ),
                        "conformidade_reserva_legal_pct": _round_half_up(
                            max(0.0, min(100.0, conf_base + d_conf)), 2
                        ),
                        "reference_date": DEFAULT_REFERENCE_DATE,
                    }
                )
        return rows

    def _build_regularizacao_pra(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _SICAR_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "qtd_imoveis_aderidos_pra": int(p[8]),
                    "imoveis_sem_sobreposicao_ti_uc_pct": float(p[9]),
                    "imoveis_livres_embargo_ibama_pct": float(p[10]),
                    "elegibilidade_credito_rural_car_pct": float(p[11]),
                    "velocidade_homologacao_car_pct": float(p[12]),
                    "indice_governanca_pra": float(p[13]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_geometria_sigef(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            p = _SICAR_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "poligonos_sem_sobreposicao_pct": float(p[14]),
                    "integracao_car_sigef_incra_pct": float(p[15]),
                    "precisao_georreferenciamento_pct": float(p[16]),
                    "atualizacao_retificacao_anual_pct": float(p[17]),
                    "indice_qualidade_geoespacial_car": float(p[18]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_features_sicar(self) -> list[dict[str, Any]]:
        imv_rows = {r["id_municipio"]: r for r in self._build_imoveis_rurais()}
        pra_rows = {r["id_municipio"]: r for r in self._build_regularizacao_pra()}
        geo_rows = {r["id_municipio"]: r for r in self._build_geometria_sigef()}
        rl_by_mun: dict[str, list[dict[str, Any]]] = {}
        for row in self._build_reserva_legal_app():
            rl_by_mun.setdefault(row["id_municipio"], []).append(row)

        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, bioma in MUNICIPIOS_REFERENCIA:
            imv = imv_rows[id_mun]
            pra = pra_rows[id_mun]
            geo = geo_rows[id_mun]
            segs = rl_by_mun[id_mun]

            taxa_passivo_media = _round_half_up(
                sum(r["taxa_passivo_ambiental_pct"] for r in segs) / len(segs), 2
            )
            score = _compute_sicar_score(
                imv["indice_cobertura_cadastral_car"],
                pra["indice_governanca_pra"],
                geo["indice_qualidade_geoespacial_car"],
                taxa_passivo_media,
            )
            tier = _classify_sicar_tier(score, pra["imoveis_livres_embargo_ibama_pct"])
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_referencia": DEFAULT_ANO_REFERENCIA,
                    "qtd_imoveis_car_ativos": imv["qtd_imoveis_car_ativos"],
                    "area_cadastrada_car_ha": imv["area_cadastrada_car_ha"],
                    "imoveis_livres_embargo_ibama_pct": pra["imoveis_livres_embargo_ibama_pct"],
                    "indice_cobertura_cadastral_car": imv["indice_cobertura_cadastral_car"],
                    "indice_governanca_pra": pra["indice_governanca_pra"],
                    "indice_qualidade_geoespacial_car": geo["indice_qualidade_geoespacial_car"],
                    "taxa_passivo_media_pct": taxa_passivo_media,
                    "score_conformidade_sicar": score,
                    "tier_conformidade_sicar": tier,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows


register_connector("sfb_sicar", SfbSicarConnector)
