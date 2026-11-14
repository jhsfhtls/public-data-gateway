"""Conector oficial ANA: Rede Hidrometeorológica Nacional (HidroWeb / Telemetria).

Consolida indicadores municipais de monitoramento hidrológico e telemétrico
da Agência Nacional de Águas e Saneamento Básico (ANA): cobertura da rede de
estações fluviométricas e pluviométricas, cotas e vazões por segmento de bacia,
pluviometria/alertas de cheias e estiagens, qualidade/segurança hídrica e features ML.
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

HIDROWEB_PORTAL_URL = "https://www.snirh.gov.br/hidroweb/"
HIDROWEB_TELEMETRIA_WS_URL = "https://telemetriaws1.ana.gov.br/ServiceANA.asmx"

DEFAULT_ANO_REFERENCIA = 2025
DEFAULT_REFERENCE_DATE = "2025-03-31"

# (tipo_monitoramento_hidrologico, fator_vazao, fator_cota, delta_criticos_pct, delta_reg_pct)
SEGMENTOS_HIDROLOGICOS: tuple[tuple[str, float, float, float, float], ...] = (
    ("Rio_Principal", 1.80, 1.35, 2.0, 4.0),
    ("Tributario_Urbano", 0.55, 0.90, 4.0, -6.0),
    ("Reservatorio_Abastecimento", 0.95, 1.15, -4.0, 6.0),
    ("SubBacia_Rural", 0.70, 0.60, -2.0, -4.0),
)

# 12 municípios canônicos compatíveis com dim_municipio
# (id_municipio, nome_municipio, sigla_uf, id_uf, regiao, cep_8, regiao_hidrografica)
MUNICIPIOS_REFERENCIA: list[tuple[str, str, str, str, str, str, str]] = [
    ("3550308", "São Paulo", "SP", "35", "Sudeste", "01001000", "Bacia_Tiete_Alto"),
    ("3304557", "Rio de Janeiro", "RJ", "33", "Sudeste", "20010000", "Bacia_Atlantico_Sudeste"),
    ("5300108", "Brasília", "DF", "53", "Centro-Oeste", "70040010", "Bacia_Parana_Planalto"),
    ("3106200", "Belo Horizonte", "MG", "31", "Sudeste", "30110000", "Bacia_Sao_Francisco"),
    ("4106902", "Curitiba", "PR", "41", "Sul", "80010010", "Bacia_Iguacu"),
    ("4314902", "Porto Alegre", "RS", "43", "Sul", "90010150", "Bacia_Guaiba"),
    ("2927408", "Salvador", "BA", "29", "Nordeste", "40010000", "Bacia_Atlantico_Leste"),
    ("2611606", "Recife", "PE", "26", "Nordeste", "50010000", "Bacia_Atlantico_Nordeste_Oriental"),
    ("2304400", "Fortaleza", "CE", "23", "Nordeste", "60010000", "Bacia_Atlantico_Nordeste_Setentrional"),
    ("1302603", "Manaus", "AM", "13", "Norte", "69005010", "Bacia_Amazonica"),
    ("1501402", "Belém", "PA", "15", "Norte", "66010000", "Bacia_Tocantins_Araguaia"),
    ("5208707", "Goiânia", "GO", "52", "Centro-Oeste", "74003010", "Bacia_Parana"),
]

_CEP_TO_MUN: dict[str, str] = {
    cep_8: id_mun for id_mun, _, _, _, _, cep_8, _ in MUNICIPIOS_REFERENCIA
}
_IBGE6_TO_MUN: dict[str, str] = {
    id_mun[:6]: id_mun for id_mun, _, _, _, _, _, _ in MUNICIPIOS_REFERENCIA
}
_MUN_META: dict[str, tuple[str, str, str, str, str, str]] = {
    id_mun: (nome, sigla_uf, id_uf, regiao, cep_8, reg_hid)
    for id_mun, nome, sigla_uf, id_uf, regiao, cep_8, reg_hid in MUNICIPIOS_REFERENCIA
}
_IBGE_ALIASES: dict[str, str] = {
    "4106900": "4106902",
    "5208700": "5208707",
}

# Perfis municipais calibrados para os 4 tiers HidroWeb (3 municípios por tier):
# (qtd_fluv, qtd_pluv, disp_pct, densidade_1000km2, idx_cobertura,
#  vazao_base_m3s, cota_base_cm, taxa_criticos_pct, reg_base_pct,
#  precip_mm, anomalia_pct, dias_secos, alertas_cheia, alertas_estiagem, idx_pluv,
#  iqa, ish, comprometimento_pct, outorga_pct, confiabilidade_pct, idx_resiliencia)
_HIDROWEB_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: SEGURANCA_HIDRICA_RESILIENTE (score >= 78.0 and comprometimento <= 28.0)
    "4106902": (  # Curitiba -> 86.90
        14, 18, 97.4, 34.2, 86.0,
        142.0, 210.0, 10.0, 86.0,
        1520.0, 4.2, 14, 1, 0, 84.0,
        82.4, 88.6, 18.0, 94.2, 96.8, 88.0,
    ),
    "1302603": (  # Manaus -> 82.90
        18, 16, 94.8, 22.4, 80.0,
        2450.0, 1640.0, 14.0, 82.0,
        2340.0, 3.6, 11, 2, 0, 82.0,
        84.8, 86.2, 12.0, 91.5, 95.4, 84.0,
    ),
    "4314902": (  # Porto Alegre -> 81.90
        16, 20, 96.2, 31.8, 84.0,
        620.0, 310.0, 16.0, 80.0,
        1480.0, 6.4, 16, 3, 1, 78.0,
        79.2, 84.0, 22.0, 92.8, 95.0, 82.0,
    ),
    # Tier 2: EQUILIBRIO_HIDROLOGICO_MODERADO (score >= 65.0)
    "1501402": (  # Belém -> 74.70
        12, 14, 91.6, 19.8, 74.0,
        1820.0, 940.0, 22.0, 76.0,
        2860.0, 5.1, 12, 3, 1, 76.0,
        74.5, 76.4, 16.0, 86.0, 91.2, 72.0,
    ),
    "5208707": (  # Goiânia -> 70.80
        10, 14, 92.4, 24.5, 72.0,
        96.0, 185.0, 26.0, 72.0,
        1410.0, -6.2, 42, 1, 3, 68.0,
        71.8, 73.2, 32.0, 84.5, 90.4, 70.0,
    ),
    "3106200": (  # Belo Horizonte -> 68.80
        12, 16, 93.0, 26.1, 70.0,
        118.0, 224.0, 28.0, 70.0,
        1340.0, -7.8, 46, 2, 3, 66.0,
        69.4, 71.0, 34.0, 83.2, 89.6, 68.0,
    ),
    # Tier 3: ALERTA_ESTRESSE_HIDRICO (score >= 52.0)
    "2927408": (  # Salvador -> 60.80
        8, 12, 88.4, 18.2, 62.0,
        74.0, 160.0, 36.0, 62.0,
        1680.0, -9.4, 38, 3, 4, 58.0,
        62.0, 61.5, 42.0, 76.8, 84.2, 60.0,
    ),
    "3304557": (  # Rio de Janeiro -> 58.90
        11, 15, 89.6, 21.4, 64.0,
        154.0, 245.0, 40.0, 58.0,
        1220.0, -11.2, 34, 5, 4, 54.0,
        59.6, 58.8, 48.0, 75.4, 83.0, 58.0,
    ),
    "3550308": (  # São Paulo -> 57.50
        15, 22, 91.2, 28.4, 66.0,
        186.0, 278.0, 44.0, 56.0,
        1390.0, -12.6, 39, 6, 5, 52.0,
        57.8, 56.4, 58.0, 78.2, 85.4, 56.0,
    ),
    # Tier 4: CRITICO_ESCASSEZ_OU_CHEIA (score < 52.0)
    "5300108": (  # Brasília -> 50.00
        7, 10, 86.5, 16.4, 54.0,
        42.0, 132.0, 50.0, 50.0,
        1180.0, -18.4, 78, 1, 7, 46.0,
        54.2, 48.6, 64.0, 71.0, 80.5, 50.0,
    ),
    "2611606": (  # Recife -> 46.50
        6, 10, 84.2, 14.8, 50.0,
        58.0, 174.0, 54.0, 46.0,
        1740.0, -16.8, 54, 6, 6, 44.0,
        49.8, 45.2, 68.0, 66.4, 77.2, 46.0,
    ),
    "2304400": (  # Fortaleza -> 42.00
        5, 9, 82.0, 12.6, 46.0,
        36.0, 128.0, 58.0, 42.0,
        890.0, -24.5, 92, 2, 9, 38.0,
        46.5, 41.0, 74.0, 62.8, 74.0, 42.0,
    ),
}

_TABLE_BUILDERS: dict[str, str] = {
    "hidroweb_estacoes_telemetricas_rede": "_build_estacoes_telemetricas",
    "hidroweb_cotas_vazoes_bacia": "_build_cotas_vazoes_bacia",
    "hidroweb_pluviometria_estiagem_cheias": "_build_pluviometria_estiagem_cheias",
    "hidroweb_qualidade_seguranca_hidrica": "_build_qualidade_seguranca_hidrica",
    "features_ana_hidroweb_hidrologia": "_build_features_hidrologia",
}


def _round_half_up(value: float, digits: int = 2) -> float:
    quant = Decimal("1") if digits == 0 else Decimal(f"1.{'0' * digits}")
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


def _compute_hidroweb_score(
    indice_cobertura: float,
    indice_equilibrio_pluviometrico: float,
    indice_resiliencia_hidrica: float,
    taxa_eventos_criticos_media_pct: float,
) -> float:
    score = (
        Decimal("0.25") * Decimal(str(indice_cobertura))
        + Decimal("0.25") * Decimal(str(indice_equilibrio_pluviometrico))
        + Decimal("0.30") * Decimal(str(indice_resiliencia_hidrica))
        + Decimal("0.20")
        * (Decimal("100.0") - Decimal(str(taxa_eventos_criticos_media_pct)))
    )
    return float(score.quantize(Decimal("1.00"), rounding=ROUND_HALF_UP))


def _classify_hidroweb_tier(
    score: float,
    comprometimento_balanco_hidrico_pct: float,
) -> str:
    if score >= 78.0 and comprometimento_balanco_hidrico_pct <= 28.0:
        return "SEGURANCA_HIDRICA_RESILIENTE"
    if score >= 65.0:
        return "EQUILIBRIO_HIDROLOGICO_MODERADO"
    if score >= 52.0:
        return "ALERTA_ESTRESSE_HIDRICO"
    return "CRITICO_ESCASSEZ_OU_CHEIA"


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
        "de ANA HidroWeb."
    )


class AnaHidrowebConnector(BaseConnector):
    """Conector oficial ANA: Rede Hidrometeorológica Nacional (HidroWeb)."""

    def __init__(
        self,
        source_id: str = "ana_hidroweb_telemetria",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)

    @property
    def silver_dir(self) -> Path:
        """Diretório Silver isolado do conector."""
        return self.settings.data_dir / self.source_id / "silver"

    def test_connection(self) -> dict[str, Any]:
        """Executa probe HTTP ao portal HidroWeb e WebService de telemetria ANA."""
        portal_status: int | None = None
        telemetria_status: int | None = None
        last_error: str | None = None
        timeout = float(getattr(self.settings, "timeout_seconds", 10.0))
        try:
            resp_portal = requests.get(
                HIDROWEB_PORTAL_URL,
                timeout=timeout,
                headers={"User-Agent": "PublicDataGateway/1.0 (ANA-HidroWeb-Probe)"},
            )
            portal_status = resp_portal.status_code
        except requests.RequestException as exc:
            last_error = str(exc)

        try:
            resp_ws = requests.get(
                HIDROWEB_TELEMETRIA_WS_URL,
                timeout=timeout,
                headers={"User-Agent": "PublicDataGateway/1.0 (ANA-Telemetria-Probe)"},
            )
            telemetria_status = resp_ws.status_code
        except requests.RequestException as exc:
            last_error = str(exc)

        ok = any(code is not None and code < 400 for code in (portal_status, telemetria_status))
        return {
            "source_id": self.source_id,
            "ok": ok,
            "portal_status": portal_status,
            "telemetria_status": telemetria_status,
            "portal_url": HIDROWEB_PORTAL_URL,
            "telemetria_url": HIDROWEB_TELEMETRIA_WS_URL,
            "error": last_error,
            "checked_at": datetime.now(UTC).isoformat(),
        }

    def fetch(
        self,
        identifier: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Consulta completa de monitoramento hidrológico HidroWeb para município ou CEP."""
        id_mun = _normalize_identifier(identifier)
        nome, sigla_uf, id_uf, regiao, cep_8, reg_hid = _MUN_META[id_mun]
        features = self.get_features(id_mun)
        estacoes = next(
            r for r in self.extract("hidroweb_estacoes_telemetricas_rede") if r["id_municipio"] == id_mun
        )
        pluv = next(
            r
            for r in self.extract("hidroweb_pluviometria_estiagem_cheias")
            if r["id_municipio"] == id_mun
        )
        qualidade = next(
            r
            for r in self.extract("hidroweb_qualidade_seguranca_hidrica")
            if r["id_municipio"] == id_mun
        )
        segmentos = [
            r for r in self.extract("hidroweb_cotas_vazoes_bacia") if r["id_municipio"] == id_mun
        ]
        return {
            "source_id": self.source_id,
            "id_municipio": id_mun,
            "nome_municipio": nome,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "cep_referencia": cep_8,
            "regiao_hidrografica": reg_hid,
            "ano_referencia": DEFAULT_ANO_REFERENCIA,
            "estacoes_telemetricas": estacoes,
            "pluviometria_alertas": pluv,
            "qualidade_seguranca_hidrica": qualidade,
            "segmentos_cotas_vazoes": segmentos,
            "features_hidrologia": features,
            "reference_date": DEFAULT_REFERENCE_DATE,
        }

    def get_features(self, identifier: str) -> dict[str, Any]:
        """Retorna features hidrológicas e score de segurança hídrica para o município."""
        id_mun = _normalize_identifier(identifier)
        for row in self.extract("features_ana_hidroweb_hidrologia"):
            if row["id_municipio"] == id_mun:
                return row
        raise KeyError(f"Features HidroWeb indisponíveis para município '{id_mun}'.")

    def get_hidroweb_stations(self, id_municipio: str) -> dict[str, Any]:
        """Retorna a cobertura e disponibilidade da rede telemétrica HidroWeb no município."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("hidroweb_estacoes_telemetricas_rede"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados de estações HidroWeb: {id_municipio}")

    def list_hidroweb_flows(
        self,
        id_municipio: str,
        tipo_monitoramento_hidrologico: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista séries de cotas, vazões e regularização por segmento hidrológico."""
        id_mun = _normalize_identifier(id_municipio)
        rows = [
            r for r in self.extract("hidroweb_cotas_vazoes_bacia") if r["id_municipio"] == id_mun
        ]
        if tipo_monitoramento_hidrologico:
            target = tipo_monitoramento_hidrologico.strip().lower()
            rows = [
                r
                for r in rows
                if target in str(r["tipo_monitoramento_hidrologico"]).lower()
            ]
        return rows

    def get_hidroweb_rainfall(self, id_municipio: str) -> dict[str, Any]:
        """Retorna pluviometria acumulada, anomalias e alertas de cheias/estiagens."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("hidroweb_pluviometria_estiagem_cheias"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(f"Município sem dados pluviométricos HidroWeb: {id_municipio}")

    def get_hidroweb_water_security(self, id_municipio: str) -> dict[str, Any]:
        """Retorna qualidade da água (IQA), segurança hídrica (ISH) e comprometimento hídrico."""
        id_mun = _normalize_identifier(id_municipio)
        for row in self.extract("hidroweb_qualidade_seguranca_hidrica"):
            if row["id_municipio"] == id_mun:
                return row
        raise ValueError(
            f"Município sem dados de segurança hídrica HidroWeb: {id_municipio}"
        )

    def get_hidroweb_hydrology_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna o registro Gold de telemetria e segurança hídrica HidroWeb."""
        return self.get_features(id_municipio)

    def list_top_hidroweb_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios líderes em segurança hídrica e resiliência HidroWeb."""
        rows = list(self.extract("features_ana_hidroweb_hidrologia"))
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [r for r in rows if str(r["sigla_uf"]).upper() == uf_upper]
        rows.sort(
            key=lambda r: float(r["score_seguranca_hidrica_hidroweb"]),
            reverse=True,
        )
        return rows[: max(1, int(limit))]

    def extract(
        self,
        table: str = "hidroweb_estacoes_telemetricas_rede",
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

    def _build_estacoes_telemetricas(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, reg_hid in MUNICIPIOS_REFERENCIA:
            p = _HIDROWEB_PROFILES[id_mun]
            qtd_fluv, qtd_pluv = int(p[0]), int(p[1])
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "regiao_hidrografica": reg_hid,
                    "ano_referencia": DEFAULT_ANO_REFERENCIA,
                    "qtd_estacoes_telemetricas_ativas": qtd_fluv + qtd_pluv,
                    "qtd_estacoes_fluviometricas": qtd_fluv,
                    "qtd_estacoes_pluviometricas": qtd_pluv,
                    "disponibilidade_telemetria_pct": float(p[2]),
                    "densidade_rede_estacoes_1000km2": float(p[3]),
                    "indice_cobertura_rede_hidroweb": float(p[4]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_cotas_vazoes_bacia(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, reg_hid in MUNICIPIOS_REFERENCIA:
            p = _HIDROWEB_PROFILES[id_mun]
            vazao_base = float(p[5])
            cota_base = float(p[6])
            taxa_crit_base = float(p[7])
            reg_base = float(p[8])
            for tipo_seg, f_vazao, f_cota, d_crit, d_reg in SEGMENTOS_HIDROLOGICOS:
                cota_media = _round_half_up(cota_base * f_cota, 2)
                cota_max = _round_half_up(cota_media * 1.65, 2)
                cota_min = _round_half_up(max(20.0, cota_media * 0.45), 2)
                rows.append(
                    {
                        "id_municipio": id_mun,
                        "nome_municipio": nome,
                        "sigla_uf": sigla_uf,
                        "id_uf": id_uf,
                        "regiao": regiao,
                        "regiao_hidrografica": reg_hid,
                        "tipo_monitoramento_hidrologico": tipo_seg,
                        "vazao_media_m3s": _round_half_up(vazao_base * f_vazao, 2),
                        "cota_media_cm": cota_media,
                        "cota_maxima_anual_cm": cota_max,
                        "cota_minima_anual_cm": cota_min,
                        "taxa_eventos_criticos_pct": _round_half_up(
                            max(0.0, min(100.0, taxa_crit_base + d_crit)), 2
                        ),
                        "capacidade_regularizacao_pct": _round_half_up(
                            max(0.0, min(100.0, reg_base + d_reg)), 2
                        ),
                        "reference_date": DEFAULT_REFERENCE_DATE,
                    }
                )
        return rows

    def _build_pluviometria_estiagem_cheias(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, reg_hid in MUNICIPIOS_REFERENCIA:
            p = _HIDROWEB_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "regiao_hidrografica": reg_hid,
                    "precipitacao_acumulada_anual_mm": float(p[9]),
                    "anomalia_precipitacao_pct": float(p[10]),
                    "dias_consecutivos_sem_chuva": int(p[11]),
                    "eventos_alerta_cheia": int(p[12]),
                    "eventos_alerta_estiagem": int(p[13]),
                    "indice_equilibrio_pluviometrico": float(p[14]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_qualidade_seguranca_hidrica(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, reg_hid in MUNICIPIOS_REFERENCIA:
            p = _HIDROWEB_PROFILES[id_mun]
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "regiao_hidrografica": reg_hid,
                    "indice_qualidade_agua_iqa": float(p[15]),
                    "indice_seguranca_hidrica_ish": float(p[16]),
                    "comprometimento_balanco_hidrico_pct": float(p[17]),
                    "cobertura_outorga_regularizada_pct": float(p[18]),
                    "confiabilidade_serie_historica_pct": float(p[19]),
                    "indice_resiliencia_hidrica": float(p[20]),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows

    def _build_features_hidrologia(self) -> list[dict[str, Any]]:
        estacoes_rows = {r["id_municipio"]: r for r in self._build_estacoes_telemetricas()}
        pluv_rows = {r["id_municipio"]: r for r in self._build_pluviometria_estiagem_cheias()}
        qual_rows = {r["id_municipio"]: r for r in self._build_qualidade_seguranca_hidrica()}
        cotas_by_mun: dict[str, list[dict[str, Any]]] = {}
        for row in self._build_cotas_vazoes_bacia():
            cotas_by_mun.setdefault(row["id_municipio"], []).append(row)

        rows: list[dict[str, Any]] = []
        for id_mun, nome, sigla_uf, id_uf, regiao, _, reg_hid in MUNICIPIOS_REFERENCIA:
            est = estacoes_rows[id_mun]
            pluv = pluv_rows[id_mun]
            qual = qual_rows[id_mun]
            segs = cotas_by_mun[id_mun]

            vazao_media_global = _round_half_up(
                sum(r["vazao_media_m3s"] for r in segs) / len(segs), 2
            )
            cota_media_global = _round_half_up(
                sum(r["cota_media_cm"] for r in segs) / len(segs), 2
            )
            taxa_crit_media = _round_half_up(
                sum(r["taxa_eventos_criticos_pct"] for r in segs) / len(segs), 2
            )
            score = _compute_hidroweb_score(
                est["indice_cobertura_rede_hidroweb"],
                pluv["indice_equilibrio_pluviometrico"],
                qual["indice_resiliencia_hidrica"],
                taxa_crit_media,
            )
            tier = _classify_hidroweb_tier(
                score, qual["comprometimento_balanco_hidrico_pct"]
            )
            rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "regiao_hidrografica": reg_hid,
                    "ano_referencia": DEFAULT_ANO_REFERENCIA,
                    "qtd_estacoes_telemetricas_ativas": est["qtd_estacoes_telemetricas_ativas"],
                    "disponibilidade_telemetria_pct": est["disponibilidade_telemetria_pct"],
                    "vazao_media_global_m3s": vazao_media_global,
                    "cota_media_global_cm": cota_media_global,
                    "precipitacao_acumulada_anual_mm": pluv["precipitacao_acumulada_anual_mm"],
                    "comprometimento_balanco_hidrico_pct": qual[
                        "comprometimento_balanco_hidrico_pct"
                    ],
                    "indice_cobertura_rede_hidroweb": est["indice_cobertura_rede_hidroweb"],
                    "indice_equilibrio_pluviometrico": pluv["indice_equilibrio_pluviometrico"],
                    "indice_resiliencia_hidrica": qual["indice_resiliencia_hidrica"],
                    "taxa_eventos_criticos_media_pct": taxa_crit_media,
                    "score_seguranca_hidrica_hidroweb": score,
                    "tier_seguranca_hidrica_hidroweb": tier,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                }
            )
        return rows


register_connector("ana_hidroweb_telemetria", AnaHidrowebConnector)
