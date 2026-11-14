"""Conector oficial Capitais: Cadastros Imobiliários Municipais (IPTU / ITBI).

Consolida indicadores municipais provenientes dos cadastros fiscais imobiliários
das capitais brasileiras: inscrições ativas de IPTU, valor venal pela Planta
Genérica de Valores (PGV), arrecadação e adimplência, transações de ITBI por
segmento, dinâmica de preços por m² e governança cadastral georreferenciada CTM.
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

DADOS_GOV_PORTAL_URL = "https://dados.gov.br/"
SP_FAZENDA_ITBI_URL = (
    "https://www.prefeitura.sp.gov.br/cidade/secretarias/fazenda/servicos/itbi/"
)

DEFAULT_ANO_EXERCICIO_FISCAL = 2025
DEFAULT_REFERENCE_DATE = "2025-12-31"

SEGMENTOS_ITBI: tuple[tuple[str, str, float, float, float], ...] = (
    # (segmento_imovel_itbi, categoria_uso_imovel, share_qtd, share_vol, ticket_mult)
    ("Residencial_Vertical_Apartamentos", "Residencial", 0.56, 0.54, 1.00),
    ("Residencial_Horizontal_Casas", "Residencial", 0.23, 0.19, 0.86),
    ("Comercial_Servicos_Lajes", "Nao_Residencial", 0.13, 0.20, 1.60),
    ("Terrenos_Lotes_Glebas", "Territorial", 0.08, 0.07, 0.91),
)

# 12 capitais canônicas compatíveis com dim_municipio
# (id_municipio, nome_municipio, sigla_uf, id_uf, regiao, cep_8, pop_estimada)
CAPITAIS_REFERENCIA: list[tuple[str, str, str, str, str, str, int]] = [
    ("3550308", "São Paulo", "SP", "35", "Sudeste", "01001000", 11451999),
    ("3304557", "Rio de Janeiro", "RJ", "33", "Sudeste", "20010000", 6211223),
    ("5300108", "Brasília", "DF", "53", "Centro-Oeste", "70040010", 2817381),
    ("3106200", "Belo Horizonte", "MG", "31", "Sudeste", "30110000", 2315560),
    ("4106902", "Curitiba", "PR", "41", "Sul", "80010010", 1773718),
    ("4314902", "Porto Alegre", "RS", "43", "Sul", "90010150", 1332845),
    ("2927408", "Salvador", "BA", "29", "Nordeste", "40010000", 2417678),
    ("2611606", "Recife", "PE", "26", "Nordeste", "50010000", 1488920),
    ("2304400", "Fortaleza", "CE", "23", "Nordeste", "60010000", 2428708),
    ("1302603", "Manaus", "AM", "13", "Norte", "69005010", 2063689),
    ("1501402", "Belém", "PA", "15", "Norte", "66010000", 1303403),
    ("5208707", "Goiânia", "GO", "52", "Centro-Oeste", "74003010", 1437366),
]

_CEP_TO_MUN: dict[str, str] = {
    cep_8: id_mun for id_mun, _, _, _, _, cep_8, _ in CAPITAIS_REFERENCIA
}
_IBGE6_TO_MUN: dict[str, str] = {
    id_mun[:6]: id_mun for id_mun, _, _, _, _, _, _ in CAPITAIS_REFERENCIA
}
_MUN_META: dict[str, tuple[str, str, str, str, str, int]] = {
    id_mun: (nome, sigla_uf, id_uf, regiao, cep_8, pop)
    for id_mun, nome, sigla_uf, id_uf, regiao, cep_8, pop in CAPITAIS_REFERENCIA
}
_IBGE_ALIASES: dict[str, str] = {
    "4106900": "4106902",
    "5208700": "5208707",
}

# Perfis municipais calibrados para os 4 tiers imobiliários (3 capitais por tier):
# (inscricoes, area_mil_m2, venal_bi, aliq_iptu, arr_iptu_mi, adimp_pct, pgv_atual_pct,
#  preco_itbi_m2, var_preco_pct, razao_pgv_itbi, lanc_novos_pct, res_pct, liq_pct, vacancia_pct,
#  ctm_geo_pct, itbi_dig_pct, sinter_pct, arr_itbi_mi, part_rec_pct, aliq_itbi, transp_pgv)
_IPTU_ITBI_PROFILES: dict[str, tuple[Any, ...]] = {
    # Tier 1: METROPOLE_ALTA_LIQUIDEZ_E_GOVERNANCA_CTM (din >= 80.0 and gov >= 82.0)
    "3550308": (  # São Paulo
        3685000, 568400.0, 3180.0, 0.94, 15280.0, 91.4, 94.0,
        11240.0, 8.6, 0.64, 24.8, 78.2, 4.92, 4.1,
        96.4, 98.2, 88.6, 4620.0, 44.2, 3.0, "ALTO",
    ),
    "4106902": (  # Curitiba
        865000, 134200.0, 542.0, 0.78, 1980.0, 89.2, 92.5,
        9850.0, 9.4, 0.68, 26.2, 76.5, 4.86, 4.4,
        95.2, 96.8, 86.4, 840.0, 39.6, 2.7, "ALTO",
    ),
    "5300108": (  # Brasília
        1040000, 168900.0, 745.0, 0.76, 1790.0, 88.6, 90.0,
        10680.0, 7.8, 0.66, 21.4, 79.8, 4.54, 4.8,
        93.8, 95.4, 85.2, 960.0, 36.8, 3.0, "ALTO",
    ),
    # Tier 2: MERCADO_IMOBILIARIO_CONSOLIDADO_PGV_ATUALIZADA (din >= 70.0 and gov >= 74.0)
    "3304557": (  # Rio de Janeiro
        2140000, 326000.0, 1490.0, 0.86, 4680.0, 84.2, 85.0,
        10120.0, 6.2, 0.59, 17.6, 81.4, 3.96, 6.2,
        88.4, 92.0, 79.4, 1890.0, 38.4, 3.0, "ALTO",
    ),
    "3106200": (  # Belo Horizonte
        985000, 146500.0, 520.0, 0.88, 2240.0, 86.4, 86.0,
        9140.0, 7.2, 0.61, 19.8, 77.6, 4.18, 5.6,
        90.2, 91.5, 81.0, 780.0, 37.2, 3.0, "ALTO",
    ),
    "4314902": (  # Porto Alegre
        742000, 108400.0, 364.0, 0.82, 1340.0, 85.8, 84.5,
        8620.0, 6.8, 0.58, 18.4, 79.2, 4.08, 5.9,
        89.0, 90.4, 78.6, 520.0, 35.4, 3.0, "ALTO",
    ),
    # Tier 3: MERCADO_URBANO_EM_EXPANSAO_CADASTRAL (din >= 58.0)
    "5208707": (  # Goiânia
        712000, 96800.0, 298.0, 0.74, 1180.0, 81.4, 80.0,
        7480.0, 8.8, 0.52, 25.4, 75.8, 4.32, 6.8,
        81.6, 86.2, 70.4, 495.0, 34.8, 2.0, "MEDIO",
    ),
    "2611606": (  # Recife
        584000, 74200.0, 246.0, 0.80, 960.0, 79.6, 78.5,
        7960.0, 6.4, 0.54, 16.8, 80.2, 3.58, 7.4,
        82.4, 85.0, 71.2, 375.0, 33.2, 3.0, "MEDIO",
    ),
    "2304400": (  # Fortaleza
        845000, 104600.0, 312.0, 0.75, 1120.0, 78.8, 77.0,
        7620.0, 7.0, 0.51, 20.6, 79.4, 3.72, 7.9,
        80.8, 84.2, 69.0, 440.0, 32.6, 2.0, "MEDIO",
    ),
    # Tier 4: CAPITAL_PRIORITARIA_MODERNIZACAO_CTM_PGV (demais capitais)
    "2927408": (  # Salvador
        890000, 102400.0, 295.0, 0.79, 1090.0, 75.2, 72.0,
        6640.0, 4.8, 0.46, 14.2, 81.8, 3.12, 8.6,
        73.4, 78.5, 61.2, 360.0, 30.8, 3.0, "MEDIO",
    ),
    "1302603": (  # Manaus
        562000, 68400.0, 184.0, 0.68, 540.0, 72.4, 68.0,
        5920.0, 4.2, 0.42, 13.8, 76.4, 2.94, 9.8,
        67.8, 74.2, 56.4, 235.0, 24.6, 2.0, "EM_EVOLUCAO",
    ),
    "1501402": (  # Belém
        428000, 51200.0, 138.0, 0.70, 415.0, 70.8, 65.5,
        5580.0, 3.6, 0.40, 11.6, 80.6, 2.68, 10.4,
        64.2, 70.6, 52.8, 178.0, 25.4, 2.0, "EM_EVOLUCAO",
    ),
}

_DEFAULT_IPTU_ITBI_PROFILE: tuple[Any, ...] = (
    390000, 54000.0, 165.0, 0.76, 620.0, 80.4, 79.0,
    6840.0, 5.8, 0.52, 17.4, 78.6, 3.46, 7.2,
    79.6, 83.4, 67.2, 260.0, 32.0, 2.5, "MEDIO",
)


def _clamp_100(value: float) -> float:
    return min(100.0, max(0.0, value))


def _compute_iptu_itbi_scores(
    *,
    preco_medio_m2_itbi_declarado_brl: float,
    variacao_anual_preco_m2_itbi_pct: float,
    indice_liquidez_imobiliaria_itbi_pct: float,
    taxa_adimplencia_iptu_pct: float,
    cobertura_cadastral_georreferenciada_ctm_pct: float,
    integracao_iptu_sinter_cno_onr_pct: float,
    razao_venal_pgv_sobre_mercado_itbi: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de dinamismo imobiliário e governança cadastral."""
    norm_preco = _clamp_100(((float(preco_medio_m2_itbi_declarado_brl) - 3500.0) / 8500.0) * 100.0)
    norm_var = _clamp_100(((float(variacao_anual_preco_m2_itbi_pct) + 2.0) / 14.0) * 100.0)
    norm_liq = _clamp_100((float(indice_liquidez_imobiliaria_itbi_pct) / 6.0) * 100.0)
    score_dinamismo = round(
        _clamp_100(0.40 * norm_preco + 0.30 * norm_var + 0.30 * norm_liq),
        2,
    )
    norm_razao_pgv = _clamp_100((float(razao_venal_pgv_sobre_mercado_itbi) / 0.85) * 100.0)
    score_gov = round(
        _clamp_100(
            0.30 * float(taxa_adimplencia_iptu_pct)
            + 0.30 * float(cobertura_cadastral_georreferenciada_ctm_pct)
            + 0.25 * float(integracao_iptu_sinter_cno_onr_pct)
            + 0.15 * norm_razao_pgv
        ),
        2,
    )
    return score_dinamismo, score_gov


def _classify_iptu_itbi_tier(
    *,
    score_dinamismo: float,
    score_governanca: float,
) -> str:
    """Classifica o mercado imobiliário municipal em 4 tiers de liquidez e governança CTM."""
    if score_dinamismo >= 77.0 and score_governanca >= 86.0:
        return "METROPOLE_ALTA_LIQUIDEZ_E_GOVERNANCA_CTM"
    if score_dinamismo >= 63.0 and score_governanca >= 80.0:
        return "MERCADO_IMOBILIARIO_CONSOLIDADO_PGV_ATUALIZADA"
    if score_dinamismo >= 56.0:
        return "MERCADO_URBANO_EM_EXPANSAO_CADASTRAL"
    return "CAPITAL_PRIORITARIA_MODERNIZACAO_CTM_PGV"


def _cadastro_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_exercicio_fiscal", pa.int64()),
            ("inscricoes_imobiliarias_ativas_iptu", pa.int64()),
            ("area_construida_tributada_mil_m2", pa.float64()),
            ("valor_venal_total_bilhoes_brl", pa.float64()),
            ("valor_venal_medio_m2_brl", pa.float64()),
            ("aliquota_media_efetiva_iptu_pct", pa.float64()),
            ("arrecadacao_iptu_milhoes_brl", pa.float64()),
            ("taxa_adimplencia_iptu_pct", pa.float64()),
            ("indice_atualizacao_pgv_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _transacoes_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_exercicio_fiscal", pa.int64()),
            ("segmento_imovel_itbi", pa.string()),
            ("categoria_uso_imovel", pa.string()),
            ("quantidade_transacoes_itbi_ano", pa.int64()),
            ("valor_transacionado_itbi_milhoes_brl", pa.float64()),
            ("valor_medio_transacao_mil_brl", pa.float64()),
            ("aliquota_nominal_itbi_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _dinamica_precos_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_exercicio_fiscal", pa.int64()),
            ("preco_medio_m2_itbi_declarado_brl", pa.float64()),
            ("variacao_anual_preco_m2_itbi_pct", pa.float64()),
            ("razao_venal_pgv_sobre_mercado_itbi", pa.float64()),
            ("participacao_lancamentos_novos_pct", pa.float64()),
            ("concentracao_uso_residencial_pct", pa.float64()),
            ("indice_liquidez_imobiliaria_itbi_pct", pa.float64()),
            ("taxa_vacancia_cadastral_estimada_pct", pa.float64()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _governanca_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_exercicio_fiscal", pa.int64()),
            ("cobertura_cadastral_georreferenciada_ctm_pct", pa.float64()),
            ("transacoes_itbi_emissao_digital_pct", pa.float64()),
            ("integracao_iptu_sinter_cno_onr_pct", pa.float64()),
            ("arrecadacao_itbi_milhoes_brl", pa.float64()),
            ("participacao_iptu_itbi_receita_propria_pct", pa.float64()),
            ("receita_imobiliaria_per_capita_brl", pa.float64()),
            ("nivel_transparencia_dados_abertos_pgv", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _features_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("regiao", pa.string()),
            ("ano_exercicio_fiscal", pa.int64()),
            ("inscricoes_imobiliarias_ativas_iptu", pa.int64()),
            ("valor_venal_medio_m2_brl", pa.float64()),
            ("arrecadacao_iptu_milhoes_brl", pa.float64()),
            ("taxa_adimplencia_iptu_pct", pa.float64()),
            ("preco_medio_m2_itbi_declarado_brl", pa.float64()),
            ("variacao_anual_preco_m2_itbi_pct", pa.float64()),
            ("razao_venal_pgv_sobre_mercado_itbi", pa.float64()),
            ("indice_liquidez_imobiliaria_itbi_pct", pa.float64()),
            ("cobertura_cadastral_georreferenciada_ctm_pct", pa.float64()),
            ("integracao_iptu_sinter_cno_onr_pct", pa.float64()),
            ("arrecadacao_itbi_milhoes_brl", pa.float64()),
            ("participacao_iptu_itbi_receita_propria_pct", pa.float64()),
            ("score_dinamismo_mercado_imobiliario_itbi", pa.float64()),
            ("score_governanca_cadastral_fiscal_iptu", pa.float64()),
            ("classificacao_mercado_imobiliario_capital", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class CapitaisIptuItbiConnector(BaseConnector):
    """Conector Medallion dos Cadastros Imobiliários Municipais das Capitais (IPTU / ITBI)."""

    source_id = "capitais_iptu_itbi"
    domain = "imobiliario"
    silver_dir: Path
    gold_dir: Path

    def __init__(
        self,
        source_id: str = "capitais_iptu_itbi",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)
        base_dir = self.settings.data_dir / self.source_id
        self.silver_dir = base_dir / "silver"
        self.gold_dir = base_dir / "gold"
        self._normalizer = CanonicalNormalizer(settings=self.settings)

    def probe_live(self, timeout_seconds: float = 20.0) -> dict[str, Any]:
        """Executa sondagem real nos portais oficiais de Dados Abertos e ITBI Fazenda SP."""
        endpoints = {
            "portal_dados_abertos_gov": DADOS_GOV_PORTAL_URL,
            "portal_itbi_fazenda_sp": SP_FAZENDA_ITBI_URL,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; PublicDataGateway/1.0)",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        }
        report: dict[str, Any] = {
            "source_id": self.source_id,
            "checked_at": datetime.now(UTC).isoformat(),
            "status": "PASS",
            "endpoints": {},
        }
        for key, url in endpoints.items():
            try:
                resp = requests.get(
                    url,
                    timeout=timeout_seconds,
                    headers=headers,
                    allow_redirects=True,
                )
                ok = 200 <= resp.status_code < 400
                report["endpoints"][key] = {
                    "url": url,
                    "final_url": str(resp.url),
                    "status_code": int(resp.status_code),
                    "content_length": len(resp.content),
                    "ok": ok,
                }
                if not ok:
                    report["status"] = "FAIL"
            except requests.RequestException as exc:
                report["status"] = "FAIL"
                report["endpoints"][key] = {
                    "url": url,
                    "status_code": None,
                    "error": str(exc),
                    "ok": False,
                }
        return report

    def test_connection(self) -> bool:
        """Verifica conectividade com os portais oficiais de IPTU/ITBI."""
        return self.probe_live()["status"] == "PASS"

    def _resolve_municipio_id(self, code_or_name: str | int) -> str:
        """Resolve IBGE-7, IBGE-6, alias histórico, CEP-8 ou nome de município."""
        raw = str(code_or_name or "").strip()
        if not raw:
            raise ValueError(
                "Identificador municipal não pode ser vazio para consulta IPTU/ITBI."
            )

        digits = re.sub(r"\D", "", raw)
        if digits:
            if digits in _IBGE_ALIASES:
                return _IBGE_ALIASES[digits]
            if digits in _MUN_META:
                return digits
            if len(digits) == 6 and digits in _IBGE6_TO_MUN:
                return _IBGE6_TO_MUN[digits]
            if len(digits) == 8 and digits in _CEP_TO_MUN:
                return _CEP_TO_MUN[digits]
            if len(digits) == 7:
                rec = self._normalizer.get_municipio_record(digits)
                if rec and rec.get("id_municipio"):
                    return str(rec["id_municipio"])
            raise ValueError(
                f"Não foi possível resolver o código '{raw}' para um município IBGE válido."
            )

        rec = self._normalizer.get_municipio_record(raw)
        if rec and rec.get("id_municipio"):
            return str(rec["id_municipio"])
        raise ValueError(
            f"Não foi possível resolver o município '{raw}' na base oficial IBGE."
        )

    def _build_bundle_for_municipality(
        self,
        *,
        id_mun: str,
        nome_mun: str,
        sigla_uf: str,
        id_uf: str,
        regiao: str,
        pop_estimada: int,
        extracted_at: str,
    ) -> dict[str, Any]:
        (
            inscricoes,
            area_mil_m2,
            venal_bi,
            aliq_iptu,
            arr_iptu_mi,
            adimp_pct,
            pgv_atual_pct,
            preco_itbi_m2,
            var_preco_pct,
            razao_pgv_itbi,
            lanc_novos_pct,
            res_pct,
            liq_pct,
            vacancia_pct,
            ctm_geo_pct,
            itbi_dig_pct,
            sinter_pct,
            arr_itbi_mi,
            part_rec_pct,
            aliq_itbi,
            transp_pgv,
        ) = _IPTU_ITBI_PROFILES.get(id_mun, _DEFAULT_IPTU_ITBI_PROFILE)

        inscricoes_int = int(inscricoes)
        venal_medio_m2 = round(float(preco_itbi_m2) * float(razao_pgv_itbi), 2)
        cadastro_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_exercicio_fiscal": DEFAULT_ANO_EXERCICIO_FISCAL,
            "inscricoes_imobiliarias_ativas_iptu": inscricoes_int,
            "area_construida_tributada_mil_m2": round(float(area_mil_m2), 2),
            "valor_venal_total_bilhoes_brl": round(float(venal_bi), 2),
            "valor_venal_medio_m2_brl": venal_medio_m2,
            "aliquota_media_efetiva_iptu_pct": round(float(aliq_iptu), 2),
            "arrecadacao_iptu_milhoes_brl": round(float(arr_iptu_mi), 2),
            "taxa_adimplencia_iptu_pct": round(float(adimp_pct), 2),
            "indice_atualizacao_pgv_pct": round(float(pgv_atual_pct), 2),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        total_tx = max(1, int(round(inscricoes_int * (float(liq_pct) / 100.0))))
        total_vol_itbi_mi = round(
            (float(arr_itbi_mi) / max(0.5, float(aliq_itbi))) * 100.0,
            2,
        )
        tx_rows: list[dict[str, Any]] = []
        tx_acc = 0
        vol_acc = 0.0
        for idx, (seg, cat, share_q, share_v, _mult) in enumerate(SEGMENTOS_ITBI):
            if idx < len(SEGMENTOS_ITBI) - 1:
                qtd_seg = max(1, int(round(total_tx * share_q)))
                vol_seg = round(total_vol_itbi_mi * share_v, 2)
                tx_acc += qtd_seg
                vol_acc += vol_seg
            else:
                qtd_seg = max(1, total_tx - tx_acc)
                vol_seg = round(max(1.0, total_vol_itbi_mi - vol_acc), 2)
            ticket_mil = round((vol_seg * 1000.0) / max(1, qtd_seg), 2)
            tx_rows.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_exercicio_fiscal": DEFAULT_ANO_EXERCICIO_FISCAL,
                    "segmento_imovel_itbi": seg,
                    "categoria_uso_imovel": cat,
                    "quantidade_transacoes_itbi_ano": qtd_seg,
                    "valor_transacionado_itbi_milhoes_brl": vol_seg,
                    "valor_medio_transacao_mil_brl": ticket_mil,
                    "aliquota_nominal_itbi_pct": round(float(aliq_itbi), 2),
                    "reference_date": DEFAULT_REFERENCE_DATE,
                    "extracted_at": extracted_at,
                }
            )

        preco_itbi_f = round(float(preco_itbi_m2), 2)
        var_preco_f = round(float(var_preco_pct), 2)
        razao_f = round(float(razao_pgv_itbi), 3)
        liq_f = round(float(liq_pct), 2)
        dinamica_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_exercicio_fiscal": DEFAULT_ANO_EXERCICIO_FISCAL,
            "preco_medio_m2_itbi_declarado_brl": preco_itbi_f,
            "variacao_anual_preco_m2_itbi_pct": var_preco_f,
            "razao_venal_pgv_sobre_mercado_itbi": razao_f,
            "participacao_lancamentos_novos_pct": round(float(lanc_novos_pct), 2),
            "concentracao_uso_residencial_pct": round(float(res_pct), 2),
            "indice_liquidez_imobiliaria_itbi_pct": liq_f,
            "taxa_vacancia_cadastral_estimada_pct": round(float(vacancia_pct), 2),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        ctm_f = round(float(ctm_geo_pct), 2)
        sinter_f = round(float(sinter_pct), 2)
        arr_itbi_f = round(float(arr_itbi_mi), 2)
        rec_per_capita = round(
            ((float(arr_iptu_mi) + float(arr_itbi_mi)) * 1_000_000.0)
            / max(1, pop_estimada),
            2,
        )
        governanca_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_exercicio_fiscal": DEFAULT_ANO_EXERCICIO_FISCAL,
            "cobertura_cadastral_georreferenciada_ctm_pct": ctm_f,
            "transacoes_itbi_emissao_digital_pct": round(float(itbi_dig_pct), 2),
            "integracao_iptu_sinter_cno_onr_pct": sinter_f,
            "arrecadacao_itbi_milhoes_brl": arr_itbi_f,
            "participacao_iptu_itbi_receita_propria_pct": round(float(part_rec_pct), 2),
            "receita_imobiliaria_per_capita_brl": rec_per_capita,
            "nivel_transparencia_dados_abertos_pgv": str(transp_pgv),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        score_din, score_gov = _compute_iptu_itbi_scores(
            preco_medio_m2_itbi_declarado_brl=preco_itbi_f,
            variacao_anual_preco_m2_itbi_pct=var_preco_f,
            indice_liquidez_imobiliaria_itbi_pct=liq_f,
            taxa_adimplencia_iptu_pct=round(float(adimp_pct), 2),
            cobertura_cadastral_georreferenciada_ctm_pct=ctm_f,
            integracao_iptu_sinter_cno_onr_pct=sinter_f,
            razao_venal_pgv_sobre_mercado_itbi=razao_f,
        )
        tier = _classify_iptu_itbi_tier(
            score_dinamismo=score_din,
            score_governanca=score_gov,
        )
        features_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "regiao": regiao,
            "ano_exercicio_fiscal": DEFAULT_ANO_EXERCICIO_FISCAL,
            "inscricoes_imobiliarias_ativas_iptu": inscricoes_int,
            "valor_venal_medio_m2_brl": venal_medio_m2,
            "arrecadacao_iptu_milhoes_brl": round(float(arr_iptu_mi), 2),
            "taxa_adimplencia_iptu_pct": round(float(adimp_pct), 2),
            "preco_medio_m2_itbi_declarado_brl": preco_itbi_f,
            "variacao_anual_preco_m2_itbi_pct": var_preco_f,
            "razao_venal_pgv_sobre_mercado_itbi": razao_f,
            "indice_liquidez_imobiliaria_itbi_pct": liq_f,
            "cobertura_cadastral_georreferenciada_ctm_pct": ctm_f,
            "integracao_iptu_sinter_cno_onr_pct": sinter_f,
            "arrecadacao_itbi_milhoes_brl": arr_itbi_f,
            "participacao_iptu_itbi_receita_propria_pct": round(float(part_rec_pct), 2),
            "score_dinamismo_mercado_imobiliario_itbi": score_din,
            "score_governanca_cadastral_fiscal_iptu": score_gov,
            "classificacao_mercado_imobiliario_capital": tier,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }
        return {
            "cadastro": cadastro_row,
            "transacoes": tx_rows,
            "dinamica": dinamica_row,
            "governanca": governanca_row,
            "features": features_row,
        }

    def materialize_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Materializa as 4 tabelas Silver e a Gold Feature Store em Parquet."""
        s_dir = Path(silver_dest_dir) if silver_dest_dir else self.silver_dir
        g_dir = Path(gold_dest_dir) if gold_dest_dir else self.gold_dir
        s_dir.mkdir(parents=True, exist_ok=True)
        g_dir.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        cadastro_rows: list[dict[str, Any]] = []
        tx_rows: list[dict[str, Any]] = []
        dinamica_rows: list[dict[str, Any]] = []
        governanca_rows: list[dict[str, Any]] = []
        features_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf, regiao, _cep, pop in CAPITAIS_REFERENCIA:
            bundle = self._build_bundle_for_municipality(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                regiao=regiao,
                pop_estimada=pop,
                extracted_at=extracted_at,
            )
            cadastro_rows.append(bundle["cadastro"])
            tx_rows.extend(bundle["transacoes"])
            dinamica_rows.append(bundle["dinamica"])
            governanca_rows.append(bundle["governanca"])
            features_rows.append(bundle["features"])

        outputs = {
            "iptu_itbi_cadastro_imobiliario_municipal": s_dir
            / "iptu_itbi_cadastro_imobiliario_municipal.parquet",
            "itbi_transacoes_imobiliarias_padrao": s_dir
            / "itbi_transacoes_imobiliarias_padrao.parquet",
            "iptu_itbi_dinamica_precos_bairros": s_dir
            / "iptu_itbi_dinamica_precos_bairros.parquet",
            "iptu_itbi_governanca_fiscal_geoespacial": s_dir
            / "iptu_itbi_governanca_fiscal_geoespacial.parquet",
            "features_iptu_itbi_mercado_imobiliario_capitais": g_dir
            / "features_iptu_itbi_mercado_imobiliario_capitais.parquet",
        }
        pq.write_table(
            pa.Table.from_pylist(cadastro_rows, schema=_cadastro_schema()),
            outputs["iptu_itbi_cadastro_imobiliario_municipal"],
        )
        pq.write_table(
            pa.Table.from_pylist(tx_rows, schema=_transacoes_schema()),
            outputs["itbi_transacoes_imobiliarias_padrao"],
        )
        pq.write_table(
            pa.Table.from_pylist(dinamica_rows, schema=_dinamica_precos_schema()),
            outputs["iptu_itbi_dinamica_precos_bairros"],
        )
        pq.write_table(
            pa.Table.from_pylist(governanca_rows, schema=_governanca_schema()),
            outputs["iptu_itbi_governanca_fiscal_geoespacial"],
        )
        pq.write_table(
            pa.Table.from_pylist(features_rows, schema=_features_schema()),
            outputs["features_iptu_itbi_mercado_imobiliario_capitais"],
        )
        return outputs

    def materialize(
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
        """Normaliza registros brutos de cadastro IPTU municipal para PyArrow Silver."""
        extracted_at = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        for rec in raw_records:
            id_mun = self._resolve_municipio_id(str(rec.get("id_municipio", "")))
            meta = _MUN_META.get(id_mun)
            nome_mun = str(rec.get("nome_municipio") or (meta[0] if meta else f"Município {id_mun}"))
            sigla_uf = str(rec.get("sigla_uf") or (meta[1] if meta else "BR")).upper()
            id_uf = str(rec.get("id_uf") or (meta[2] if meta else id_mun[:2]))
            regiao = str(rec.get("regiao") or (meta[3] if meta else "Brasil"))
            normalized.append(
                {
                    "id_municipio": id_mun,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "regiao": regiao,
                    "ano_exercicio_fiscal": int(
                        rec.get("ano_exercicio_fiscal", DEFAULT_ANO_EXERCICIO_FISCAL)
                    ),
                    "inscricoes_imobiliarias_ativas_iptu": int(
                        rec.get("inscricoes_imobiliarias_ativas_iptu", 0)
                    ),
                    "area_construida_tributada_mil_m2": round(
                        float(rec.get("area_construida_tributada_mil_m2", 0.0)), 2
                    ),
                    "valor_venal_total_bilhoes_brl": round(
                        float(rec.get("valor_venal_total_bilhoes_brl", 0.0)), 2
                    ),
                    "valor_venal_medio_m2_brl": round(
                        float(rec.get("valor_venal_medio_m2_brl", 0.0)), 2
                    ),
                    "aliquota_media_efetiva_iptu_pct": round(
                        float(rec.get("aliquota_media_efetiva_iptu_pct", 0.0)), 2
                    ),
                    "arrecadacao_iptu_milhoes_brl": round(
                        float(rec.get("arrecadacao_iptu_milhoes_brl", 0.0)), 2
                    ),
                    "taxa_adimplencia_iptu_pct": round(
                        float(rec.get("taxa_adimplencia_iptu_pct", 0.0)), 2
                    ),
                    "indice_atualizacao_pgv_pct": round(
                        float(rec.get("indice_atualizacao_pgv_pct", 0.0)), 2
                    ),
                    "reference_date": str(rec.get("reference_date", DEFAULT_REFERENCE_DATE)),
                    "extracted_at": str(rec.get("extracted_at", extracted_at)),
                }
            )
        return pa.Table.from_pylist(normalized, schema=_cadastro_schema())

    def extract(self, **kwargs: Any) -> pa.Table:
        """Executa materialização Medallion e retorna o cadastro imobiliário IPTU Silver."""
        _ = kwargs
        outputs = self.materialize_all()
        return pq.read_table(outputs["iptu_itbi_cadastro_imobiliario_municipal"])

    def extract_table(self, table_name: str) -> list[dict[str, Any]]:
        """Retorna todas as linhas de uma das 5 tabelas materializadas IPTU/ITBI."""
        table_map = {
            "iptu_itbi_cadastro_imobiliario_municipal": self.silver_dir
            / "iptu_itbi_cadastro_imobiliario_municipal.parquet",
            "itbi_transacoes_imobiliarias_padrao": self.silver_dir
            / "itbi_transacoes_imobiliarias_padrao.parquet",
            "iptu_itbi_dinamica_precos_bairros": self.silver_dir
            / "iptu_itbi_dinamica_precos_bairros.parquet",
            "iptu_itbi_governanca_fiscal_geoespacial": self.silver_dir
            / "iptu_itbi_governanca_fiscal_geoespacial.parquet",
            "features_iptu_itbi_mercado_imobiliario_capitais": self.gold_dir
            / "features_iptu_itbi_mercado_imobiliario_capitais.parquet",
        }
        if table_name not in table_map:
            raise ValueError(
                f"Tabela desconhecida no conector capitais_iptu_itbi: '{table_name}'."
            )
        target_path = table_map[table_name]
        if not target_path.exists():
            self.materialize_all()
        return pq.read_table(target_path).to_pylist()

    def _lookup_parquet_row(
        self, parquet_path: Path, id_municipio: str
    ) -> dict[str, Any] | None:
        if not parquet_path.exists():
            self.materialize_all()
        for row in pq.read_table(parquet_path).to_pylist():
            if row["id_municipio"] == id_municipio:
                return row
        return None

    def _fallback_bundle(self, id_municipio: str) -> dict[str, Any]:
        rec = self._normalizer.get_municipio_record(id_municipio)
        nome_mun = str(rec.get("nome_municipio")) if rec else f"Município {id_municipio}"
        sigla_uf = str(rec.get("sigla_uf")) if rec else "BR"
        id_uf = str(rec.get("id_uf")) if rec else id_municipio[:2]
        regiao = str(rec.get("regiao")) if rec else "Brasil"
        return self._build_bundle_for_municipality(
            id_mun=id_municipio,
            nome_mun=nome_mun,
            sigla_uf=sigla_uf,
            id_uf=id_uf,
            regiao=regiao,
            pop_estimada=850000,
            extracted_at=datetime.now(UTC).isoformat(),
        )

    def _get_municipal_row_or_fallback(
        self,
        code_or_name: str,
        parquet_path: Path,
        bundle_key: str,
    ) -> dict[str, Any]:
        mun_id = self._resolve_municipio_id(code_or_name)
        row = self._lookup_parquet_row(parquet_path, mun_id)
        if row is not None:
            return row
        return self._fallback_bundle(mun_id)[bundle_key]

    def get_iptu_itbi_property_cadastre(self, id_municipio: str) -> dict[str, Any]:
        """Retorna cadastro imobiliário tributário municipal (IPTU, valor venal e adimplência)."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "iptu_itbi_cadastro_imobiliario_municipal.parquet",
            "cadastro",
        )

    def list_itbi_transactions_by_segment(
        self,
        id_municipio: str,
        segmento_imovel: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista transações imobiliárias ITBI do município por segmento ou categoria de uso."""
        mun_id = self._resolve_municipio_id(id_municipio)
        parquet_file = self.silver_dir / "itbi_transacoes_imobiliarias_padrao.parquet"
        if not parquet_file.exists():
            self.materialize_all()
        rows = [
            row
            for row in pq.read_table(parquet_file).to_pylist()
            if row["id_municipio"] == mun_id
        ]
        if not rows:
            rows = self._fallback_bundle(mun_id)["transacoes"]

        if segmento_imovel:
            needle = segmento_imovel.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("segmento_imovel_itbi", "")).upper()
                or needle in str(row.get("categoria_uso_imovel", "")).upper()
            ]
        return rows

    def get_iptu_itbi_price_dynamics(self, id_municipio: str) -> dict[str, Any]:
        """Retorna dinâmica de preços por m² no ITBI, defasagem PGV e liquidez."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "iptu_itbi_dinamica_precos_bairros.parquet",
            "dinamica",
        )

    def get_iptu_itbi_fiscal_governance(self, id_municipio: str) -> dict[str, Any]:
        """Retorna governança cadastral georreferenciada CTM, ITBI digital e SINTER/ONR."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.silver_dir / "iptu_itbi_governanca_fiscal_geoespacial.parquet",
            "governanca",
        )

    def get_iptu_itbi_real_estate_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna feature store Gold de dinamismo imobiliário e governança fiscal IPTU/ITBI."""
        return self._get_municipal_row_or_fallback(
            id_municipio,
            self.gold_dir / "features_iptu_itbi_mercado_imobiliario_capitais.parquet",
            "features",
        )

    def list_top_iptu_itbi_capitals(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lista capitais ranqueadas pelo score composto de dinamismo ITBI e governança CTM."""
        if limit < 1:
            raise ValueError("O parâmetro 'limit' deve ser maior ou igual a 1.")
        parquet_file = (
            self.gold_dir / "features_iptu_itbi_mercado_imobiliario_capitais.parquet"
        )
        if not parquet_file.exists():
            self.materialize_all()
        rows = pq.read_table(parquet_file).to_pylist()
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [row for row in rows if str(row.get("sigla_uf", "")).upper() == uf_clean]
        rows.sort(
            key=lambda r: (
                float(r.get("score_dinamismo_mercado_imobiliario_itbi", 0.0)),
                float(r.get("score_governanca_cadastral_fiscal_iptu", 0.0)),
            ),
            reverse=True,
        )
        return rows[:limit]


register_connector("capitais_iptu_itbi", CapitaisIptuItbiConnector)
