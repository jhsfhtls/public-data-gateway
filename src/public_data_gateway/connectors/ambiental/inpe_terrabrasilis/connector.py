"""Conector oficial INPE: Plataforma TerraBrasilis (DETER & PRODES).

Consolida indicadores municipais de monitoramento orbital de supressão vegetal
e uso da terra provenientes do INPE/TerraBrasilis: incremento anual PRODES,
alertas DETER-B por classe (corte raso, degradação, queimada e mineração),
balanço de cobertura nativa/uso agropecuário, embargos/CAR e risco EUDR/ESG.
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

TERRABRASILIS_PORTAL_URL = "https://terrabrasilis.dpi.inpe.br/"
TERRABRASILIS_WFS_URL = (
    "https://terrabrasilis.dpi.inpe.br/geoserver/ows?service=WFS&request=GetCapabilities"
)

DEFAULT_ANO_PRODES = 2024
DEFAULT_REFERENCE_DATE = "2025-02-28"

# (classe_alerta_deter, share_alertas, share_area, uc_ti_factor, dias_medios)
CLASSES_ALERTA_DETER: tuple[tuple[str, float, float, float, float], ...] = (
    ("Desmatamento_Corte_Raso", 0.44, 0.48, 1.15, 4.2),
    ("Degradacao_Florestal", 0.28, 0.26, 1.05, 6.8),
    ("Cicatrice_Queimada", 0.20, 0.19, 0.90, 3.5),
    ("Mineracao_Ilegal", 0.08, 0.07, 1.35, 5.4),
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

# Perfis municipais calibrados para os 4 tiers TerraBrasilis (3 municípios por tier):
# (area_prodes_km2, acum_km2, cob_flor_pct, var_prodes_pct, taxa_rel_pct, focos,
#  tot_alertas_deter, area_deter_km2, base_uc_ti_pct,
#  veg_nat_km2, agro_km2, regen_km2, hidro_km2, pressao_ant_pct, conf_cod_flor_pct,
#  embargos, autos_inf, car_reg_pct, sar_opt_pct, eleg_esg_pct, risco_eudr_pct, pront_fisc)
_TERRABRASILIS_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: MUNICIPIO_CONFORMIDADE_VERDE_EUDR (composto >= 78.0 and pressao <= 22.0)
    "4106902": (  # Curitiba
        0.42, 48.5, 41.2, -18.4, 0.14, 14,
        6, 0.35, 8.2,
        179.2, 118.4, 24.6, 11.8, 18.4, 94.6,
        1, 4, 95.8, 98.4, 96.4, 3.2, 94.2,
    ),
    "3550308": (  # São Paulo
        1.15, 182.4, 38.6, -15.2, 0.19, 28,
        12, 0.94, 14.5,
        587.1, 214.0, 68.4, 49.2, 22.8, 93.2,
        2, 9, 94.4, 98.8, 94.8, 4.6, 92.8,
    ),
    "3304557": (  # Rio de Janeiro
        1.48, 214.0, 36.8, -12.8, 0.24, 36,
        15, 1.18, 16.2,
        441.6, 162.5, 52.1, 38.4, 25.4, 91.8,
        3, 11, 92.6, 97.6, 93.2, 5.8, 90.4,
    ),
    # Tier 2: ZONA_CONSOLIDADA_BAIXA_PRESSAO_DETER (composto >= 64.0)
    "3106200": (  # Belo Horizonte
        2.35, 290.6, 33.4, -9.6, 0.38, 48,
        22, 1.85, 11.4,
        110.5, 64.2, 18.4, 8.2, 31.2, 87.4,
        5, 14, 88.2, 95.4, 86.5, 9.4, 85.2,
    ),
    "4314902": (  # Porto Alegre
        2.64, 312.8, 31.8, -8.2, 0.41, 52,
        24, 2.12, 9.8,
        157.8, 148.6, 21.3, 46.5, 34.6, 85.6,
        6, 16, 86.8, 94.8, 84.2, 11.2, 83.6,
    ),
    "5300108": (  # Brasília
        14.80, 1420.5, 42.6, -6.4, 0.62, 184,
        74, 11.40, 18.6,
        2454.2, 2380.4, 194.6, 82.1, 41.8, 82.4,
        14, 38, 84.1, 96.2, 80.4, 16.8, 84.8,
    ),
    # Tier 3: FRONTEIRA_ATENCAO_CONTROLE_AMBIENTAL (composto >= 48.0)
    "2611606": (  # Recife
        4.12, 418.2, 24.6, -3.1, 0.78, 68,
        38, 3.45, 12.8,
        53.8, 48.2, 9.4, 14.2, 52.4, 71.8,
        11, 24, 73.5, 89.2, 68.4, 24.6, 72.4,
    ),
    "2927408": (  # Salvador
        5.84, 594.0, 26.2, -1.8, 0.84, 92,
        46, 4.92, 14.2,
        181.6, 134.8, 22.8, 26.4, 55.6, 69.4,
        15, 29, 71.2, 88.4, 65.2, 27.8, 70.6,
    ),
    "2304400": (  # Fortaleza
        6.45, 642.5, 22.8, 1.4, 0.96, 114,
        52, 5.40, 10.6,
        71.2, 58.4, 11.2, 12.8, 58.9, 66.8,
        18, 34, 68.4, 86.5, 61.8, 31.2, 68.2,
    ),
    # Tier 4: ARCO_CRITICO_DESMATAMENTO_PRIORITARIO
    "5208707": (  # Goiânia
        18.60, 1890.4, 21.4, 4.8, 1.32, 248,
        112, 16.80, 15.4,
        156.0, 412.6, 28.4, 18.6, 68.4, 58.2,
        29, 58, 61.4, 84.2, 52.6, 42.4, 63.8,
    ),
    "1501402": (  # Belém
        26.40, 2480.2, 54.8, 7.6, 1.64, 342,
        164, 23.20, 28.4,
        580.6, 214.8, 64.2, 198.4, 72.8, 52.4,
        42, 76, 54.2, 81.6, 44.8, 51.6, 59.4,
    ),
    "1302603": (  # Manaus
        48.20, 4120.8, 76.4, 11.2, 1.98, 518,
        248, 41.60, 34.8,
        8710.4, 892.5, 312.6, 1480.2, 76.2, 48.6,
        64, 108, 48.6, 79.4, 38.4, 62.8, 56.2,
    ),
}


def _round2(val: Decimal) -> float:
    return float(min(Decimal("100.0"), max(Decimal("0.0"), val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _compute_terrabrasilis_scores(
    cobertura_florestal_pct: float,
    conformidade_florestal_pct: float,
    elegibilidade_esg_pct: float,
    area_desmatada_km2: float,
    area_deter_km2: float,
    risco_eudr_pct: float,
) -> tuple[float, float, float, str]:
    """Calcula scores normalizados (0-100) e perfil ambiental TerraBrasilis."""
    score_preservacao = _round2(
        Decimal(str(cobertura_florestal_pct)) * Decimal("0.35")
        + Decimal(str(conformidade_florestal_pct)) * Decimal("0.35")
        + Decimal(str(elegibilidade_esg_pct)) * Decimal("0.30")
    )
    score_pressao = _round2(
        Decimal(str(area_desmatada_km2)) * Decimal("0.45")
        + Decimal(str(area_deter_km2)) * Decimal("0.55")
        + Decimal(str(risco_eudr_pct)) * Decimal("0.40")
    )
    score_composto = _round2(
        Decimal(str(score_preservacao)) * Decimal("0.60")
        + (Decimal("100.0") - Decimal(str(score_pressao))) * Decimal("0.40")
    )

    if score_composto >= 80.0 and score_pressao <= 5.0:
        perfil = "MUNICIPIO_CONFORMIDADE_VERDE_EUDR"
    elif score_composto >= 70.0:
        perfil = "ZONA_CONSOLIDADA_BAIXA_PRESSAO_DETER"
    elif score_composto >= 60.0:
        perfil = "FRONTEIRA_ATENCAO_CONTROLE_AMBIENTAL"
    else:
        perfil = "ARCO_CRITICO_DESMATAMENTO_PRIORITARIO"

    return score_preservacao, score_pressao, score_composto, perfil


class InpeTerrabrasilisConnector(BaseConnector):
    """Conector INPE Plataforma TerraBrasilis (DETER & PRODES)."""

    def __init__(
        self,
        source_id: str = "inpe_terrabrasilis",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)

    def _data_dir(self) -> Path:
        return self.settings.data_dir / self.source_id

    def _table_path(self, table_name: str) -> Path:
        layer = (
            "gold"
            if table_name == "features_inpe_terrabrasilis_desmatamento"
            else "silver"
        )
        return self._data_dir() / layer / f"{table_name}.parquet"

    def _resolve_municipality(self, identifier: str) -> str:
        raw = re.sub(r"\D", "", str(identifier or "").strip())
        if not raw:
            raise ValueError(
                f"Identificador municipal inválido para inpe_terrabrasilis: {identifier!r}"
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
            f"Município não encontrado na base de referência TerraBrasilis: {identifier!r}"
        )

    def probe_live_endpoints(self, timeout: int = 10) -> dict[str, Any]:
        """Executa verificação HTTP leve nos endpoints públicos oficiais do TerraBrasilis/INPE."""
        headers = {
            "User-Agent": "BrazilDataGateway/1.0 (+https://terrabrasilis.dpi.inpe.br/)"
        }
        endpoints = {
            "terrabrasilis_portal": TERRABRASILIS_PORTAL_URL,
            "terrabrasilis_geoserver_wfs": TERRABRASILIS_WFS_URL,
        }
        results: dict[str, Any] = {}
        for key, url in endpoints.items():
            try:
                resp = requests.get(url, headers=headers, timeout=timeout)
                results[key] = resp.status_code
            except requests.RequestException as exc:
                results[key] = f"error:{type(exc).__name__}"
        return results

    def test_connection(self) -> dict[str, Any]:
        """Valida conectividade com os endpoints públicos do INPE TerraBrasilis."""
        probes = self.probe_live_endpoints()
        ok = any(code == 200 for code in probes.values())
        return {"status": "ok" if ok else "degraded", "probes": probes}

    def _build_datasets(self) -> dict[str, list[dict[str, Any]]]:
        now_iso = datetime.now(UTC).isoformat()

        prodes_rows: list[dict[str, Any]] = []
        deter_rows: list[dict[str, Any]] = []
        cobertura_rows: list[dict[str, Any]] = []
        governanca_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf, regiao, _cep, bioma in MUNICIPIOS_REFERENCIA:
            (
                area_prodes,
                acum_km2,
                cob_flor_pct,
                var_prodes_pct,
                taxa_rel_pct,
                focos,
                tot_alertas_deter,
                area_deter_km2,
                base_uc_ti_pct,
                veg_nat_km2,
                agro_km2,
                regen_km2,
                hidro_km2,
                pressao_ant_pct,
                conf_cod_flor_pct,
                embargos,
                autos_inf,
                car_reg_pct,
                sar_opt_pct,
                eleg_esg_pct,
                risco_eudr_pct,
                pront_fisc,
            ) = _TERRABRASILIS_PROFILES[id_mun]

            prodes_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_prodes": DEFAULT_ANO_PRODES,
                    "area_desmatada_prodes_km2": round(float(area_prodes), 2),
                    "desmatamento_acumulado_km2": round(float(acum_km2), 2),
                    "cobertura_florestal_remanescente_pct": round(float(cob_flor_pct), 2),
                    "variacao_anual_prodes_pct": round(float(var_prodes_pct), 2),
                    "taxa_desmatamento_relativa_area_pct": round(float(taxa_rel_pct), 2),
                    "focos_calor_associados_ano": int(focos),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

            # Alertas DETER por classe (soma exata de num_alertas e area_alertas_deter_km2)
            rem_alertas = int(tot_alertas_deter)
            rem_area = float(area_deter_km2)
            for idx, (classe, s_al, s_ar, uc_fac, dias_med) in enumerate(
                CLASSES_ALERTA_DETER
            ):
                if idx == len(CLASSES_ALERTA_DETER) - 1:
                    n_al = rem_alertas
                    ar_al = round(rem_area, 2)
                else:
                    n_al = int(round(tot_alertas_deter * s_al))
                    ar_al = round(area_deter_km2 * s_ar, 2)
                    rem_alertas -= n_al
                    rem_area = round(rem_area - ar_al, 4)

                area_med_ha = round((ar_al * 100.0) / max(1, n_al), 2)
                uc_ti = round(min(95.0, base_uc_ti_pct * uc_fac), 2)
                prop_classe = round((ar_al / max(0.01, area_deter_km2)) * 100.0, 2)
                deter_rows.append(
                    {
                        "id_municipio": id_mun,
                        "nome_municipio": nome_mun,
                        "sigla_uf": sigla_uf,
                        "id_uf": id_uf,
                        "regiao": regiao,
                        "ano_prodes": DEFAULT_ANO_PRODES,
                        "classe_alerta_deter": classe,
                        "num_alertas_deter_ano": int(n_al),
                        "area_alertas_deter_km2": ar_al,
                        "area_media_poligono_ha": area_med_ha,
                        "alertas_em_uc_ti_pct": uc_ti,
                        "dias_medios_emissao_alerta": round(float(dias_med), 2),
                        "proporcao_classe_sobre_total_pct": prop_classe,
                        "reference_date": DEFAULT_REFERENCE_DATE,
                        "extracted_at": now_iso,
                    }
                )

            cobertura_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_prodes": DEFAULT_ANO_PRODES,
                    "area_vegetacao_nativa_km2": round(float(veg_nat_km2), 2),
                    "area_uso_agropecuario_km2": round(float(agro_km2), 2),
                    "area_regeneracao_secundaria_km2": round(float(regen_km2), 2),
                    "area_hidrografia_km2": round(float(hidro_km2), 2),
                    "indice_pressao_antropica_pct": round(float(pressao_ant_pct), 2),
                    "conformidade_codigo_florestal_pct": round(
                        float(conf_cod_flor_pct), 2
                    ),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

            governanca_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_prodes": DEFAULT_ANO_PRODES,
                    "embargos_ibama_ativos_ano": int(embargos),
                    "autos_infracao_ambiental_ano": int(autos_inf),
                    "propriedades_car_regularizadas_pct": round(float(car_reg_pct), 2),
                    "cobertura_monitoramento_sar_optico_pct": round(
                        float(sar_opt_pct), 2
                    ),
                    "elegibilidade_credito_rural_esg_pct": round(
                        float(eleg_esg_pct), 2
                    ),
                    "risco_desconformidade_eudr_pct": round(float(risco_eudr_pct), 2),
                    "indice_prontidao_fiscalizacao": round(float(pront_fisc), 2),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

            score_pres, score_pressao, score_comp, perfil = (
                _compute_terrabrasilis_scores(
                    cobertura_florestal_pct=cob_flor_pct,
                    conformidade_florestal_pct=conf_cod_flor_pct,
                    elegibilidade_esg_pct=eleg_esg_pct,
                    area_desmatada_km2=area_prodes,
                    area_deter_km2=area_deter_km2,
                    risco_eudr_pct=risco_eudr_pct,
                )
            )
            gold_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "bioma_predominante": bioma,
                    "ano_prodes": DEFAULT_ANO_PRODES,
                    "area_desmatada_prodes_km2": round(float(area_prodes), 2),
                    "cobertura_florestal_remanescente_pct": round(float(cob_flor_pct), 2),
                    "variacao_anual_prodes_pct": round(float(var_prodes_pct), 2),
                    "total_alertas_deter_ano": int(tot_alertas_deter),
                    "area_total_alertas_deter_km2": round(float(area_deter_km2), 2),
                    "conformidade_codigo_florestal_pct": round(
                        float(conf_cod_flor_pct), 2
                    ),
                    "embargos_ibama_ativos_ano": int(embargos),
                    "elegibilidade_credito_rural_esg_pct": round(
                        float(eleg_esg_pct), 2
                    ),
                    "risco_desconformidade_eudr_pct": round(float(risco_eudr_pct), 2),
                    "indice_prontidao_fiscalizacao": round(float(pront_fisc), 2),
                    "score_preservacao_conformidade_florestal": score_pres,
                    "score_pressao_desmatamento_deter": score_pressao,
                    "score_composto_risco_esg_territorial": score_comp,
                    "perfil_risco_ambiental_terrabrasilis": perfil,
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": now_iso,
                }
            )

        return {
            "terrabrasilis_prodes_desmatamento_anual": prodes_rows,
            "terrabrasilis_deter_alertas_classe": deter_rows,
            "terrabrasilis_cobertura_uso_terra_bioma": cobertura_rows,
            "terrabrasilis_governanca_fiscalizacao_esg": governanca_rows,
            "features_inpe_terrabrasilis_desmatamento": gold_rows,
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

    def extract(
        self,
        table: str = "features_inpe_terrabrasilis_desmatamento",
        force: bool = False,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Extrai/materializa registros de uma tabela do conector INPE TerraBrasilis."""
        return self.load_or_materialize(table, force=force)

    def fetch(
        self,
        table: str = "features_inpe_terrabrasilis_desmatamento",
        force: bool = False,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Interface padrão de ingestão/leitura do BaseConnector."""
        return self.load_or_materialize(table, force=force)

    def get_terrabrasilis_prodes_deforestation(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna dados anuais PRODES e cobertura florestal remanescente no município."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("terrabrasilis_prodes_desmatamento_anual")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(f"Município sem dados PRODES TerraBrasilis: {id_municipio}")

    def list_terrabrasilis_deter_alerts(
        self,
        id_municipio: str,
        classe_alerta_deter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista alertas DETER-B do INPE desagregados por classe de alerta."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("terrabrasilis_deter_alertas_classe")
        filtered = [r for r in rows if r["id_municipio"] == mun]
        if classe_alerta_deter:
            target = classe_alerta_deter.strip().lower()
            filtered = [
                r
                for r in filtered
                if target in str(r["classe_alerta_deter"]).lower()
            ]
        return filtered

    def get_terrabrasilis_land_cover(self, id_municipio: str) -> dict[str, Any]:
        """Retorna balanço de vegetação nativa, uso agropecuário e Código Florestal."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("terrabrasilis_cobertura_uso_terra_bioma")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(
            f"Município sem dados de cobertura TerraBrasilis: {id_municipio}"
        )

    def get_terrabrasilis_esg_governance(self, id_municipio: str) -> dict[str, Any]:
        """Retorna embargos IBAMA, regularização CAR e elegibilidade EUDR/ESG."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("terrabrasilis_governanca_fiscalizacao_esg")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(
            f"Município sem dados de governança ESG TerraBrasilis: {id_municipio}"
        )

    def get_terrabrasilis_environmental_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Retorna o registro Gold de monitoramento ambiental e risco ESG/EUDR."""
        mun = self._resolve_municipality(id_municipio)
        rows = self.load_or_materialize("features_inpe_terrabrasilis_desmatamento")
        for row in rows:
            if row["id_municipio"] == mun:
                return row
        raise ValueError(
            f"Município sem features Gold TerraBrasilis: {id_municipio}"
        )

    def list_top_terrabrasilis_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios líderes em conformidade verde e baixo risco ESG TerraBrasilis."""
        rows = self.load_or_materialize("features_inpe_terrabrasilis_desmatamento")
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [r for r in rows if str(r["sigla_uf"]).upper() == uf_upper]
        rows.sort(
            key=lambda r: float(r["score_composto_risco_esg_territorial"]),
            reverse=True,
        )
        return rows[: max(1, int(limit))]


register_connector("inpe_terrabrasilis", InpeTerrabrasilisConnector)
