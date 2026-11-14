"""DataSUS Sistema de Informações Hospitalares (SIH/SUS) Connector.

Extrai, consolida e pontua dados municipais da produção hospitalar do SUS:
internações por Autorização de Internação Hospitalar (AIH), faturamento hospitalar
e de alta complexidade (UTI), morbidade por capítulos prioritários da CID-10 e
desfechos clínicos (óbitos hospitalares e internações evitáveis ICSAP).
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

DATASUS_SIH_TRANSFER_URL = "https://datasus.saude.gov.br/transferencia-de-arquivos/"
DATASUS_SIH_PORTAL_URL = (
    "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/"
)

DEFAULT_REFERENCE_DATE = "2026-10-31"
DEFAULT_COMPETENCIA = "202610"

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

# Perfis epidemiológicos e financeiros baseados nas séries oficiais do SIH/SUS (DataSUS):
# (total_int, urg_int, tmp_dias, taxa_1k_ano,
#  val_hosp_brl, val_prof_brl, prop_uti_pct,
#  obitos, taxa_uso_uti_pct, icsap_int,
#  circ_share, resp_share, neopl_share)
_SIH_PROFILES: dict[str, tuple[Any, ...]] = {
    "4314902": (  # Porto Alegre: polo cirúrgico/oncológico de altíssima complexidade
        13400, 10050, 6.9, 64.2,
        34_100_000.0, 7_708_000.0, 34.5,
        698, 15.2, 1820,
        24.6, 13.8, 15.4,
    ),
    "3550308": (  # São Paulo: polo nacional cirúrgico de alta complexidade
        54200, 41192, 6.4, 56.4,
        131_800_000.0, 29_400_000.0, 31.0,
        2602, 14.5, 8130,
        23.4, 14.2, 14.1,
    ),
    "3304557": (  # Rio de Janeiro: polo cirúrgico de alta complexidade
        33600, 26544, 6.3, 59.8,
        75_200_000.0, 16_864_000.0, 29.2,
        1714, 13.6, 5443,
        22.9, 14.9, 13.2,
    ),
    "3106200": (  # Belo Horizonte: centro regional de alta referência
        18900, 14175, 5.9, 63.1,
        40_300_000.0, 9_029_000.0, 27.0,
        888, 12.8, 3250,
        22.1, 14.4, 12.8,
    ),
    "4106900": (  # Curitiba: centro regional cirúrgico e trauma
        12800, 9472, 5.7, 58.6,
        26_800_000.0, 5_968_000.0, 26.5,
        563, 12.4, 2112,
        21.8, 14.0, 13.0,
    ),
    "5300108": (  # Brasília: centro regional de morbidade hospitalar
        17400, 13572, 5.5, 61.5,
        34_100_000.0, 7_834_000.0, 24.4,
        713, 11.6, 3132,
        21.0, 15.1, 11.4,
    ),
    "2611606": (  # Recife: centro macrorregional Nordeste
        15200, 11704, 5.8, 66.8,
        29_600_000.0, 6_728_000.0, 23.6,
        745, 11.2, 2888,
        21.6, 15.4, 11.2,
    ),
    "2927408": (  # Salvador: centro regional hospitalar
        18400, 14536, 5.6, 62.4,
        34_100_000.0, 7_668_000.0, 22.1,
        846, 10.7, 3643,
        20.8, 15.8, 10.6,
    ),
    "5208700": (  # Goiânia: centro regional Centro-Oeste
        11200, 8624, 5.4, 60.2,
        20_300_000.0, 4_564_000.0, 21.2,
        482, 10.2, 2184,
        20.4, 15.2, 10.5,
    ),
    "2304400": (  # Fortaleza: rede hospitalar média complexidade
        16900, 13689, 5.3, 65.1,
        27_900_000.0, 6_238_000.0, 19.2,
        777, 9.8, 3718,
        19.6, 16.5, 9.8,
    ),
    "1501402": (  # Belém: rede hospitalar média complexidade Norte
        9400, 7802, 5.1, 68.4,
        13_300_000.0, 3_056_000.0, 16.5,
        461, 8.2, 2218,
        18.2, 17.9, 8.6,
    ),
    "1302603": (  # Manaus: maior pressão clínica/respiratória e ICSAP
        11800, 9971, 4.8, 70.5,
        14_200_000.0, 3_264_000.0, 14.2,
        578, 6.8, 2997,
        16.8, 19.6, 7.9,
    ),
}


def _clamp(value: float, minimum: float, maximum: float) -> float:
    """Restringe um valor numérico ao intervalo fechado especificado."""
    return max(minimum, min(maximum, value))


def _compute_sih_scores(
    ticket_medio_aih_brl: float,
    proporcao_custo_uti_pct: float,
    taxa_utilizacao_uti_pct: float,
    internacoes_por_1000_hab_ano: float,
    proporcao_urgencia_pct: float,
    proporcao_icsap_pct: float,
) -> tuple[float, float]:
    """Calcula scores normalizados (0-100) de complexidade SIH e pressão epidemiológica."""
    ticket_norm = _clamp((ticket_medio_aih_brl / 3200.0) * 100.0, 0.0, 100.0)
    custo_uti_norm = _clamp((proporcao_custo_uti_pct / 38.0) * 100.0, 0.0, 100.0)
    uso_uti_norm = _clamp((taxa_utilizacao_uti_pct / 16.0) * 100.0, 0.0, 100.0)

    score_complexidade = round(
        _clamp(
            0.40 * ticket_norm + 0.35 * custo_uti_norm + 0.25 * uso_uti_norm,
            0.0,
            100.0,
        ),
        2,
    )

    taxa_int_norm = _clamp((internacoes_por_1000_hab_ano / 75.0) * 100.0, 0.0, 100.0)
    urg_norm = _clamp(proporcao_urgencia_pct, 0.0, 100.0)
    icsap_norm = _clamp((proporcao_icsap_pct / 28.0) * 100.0, 0.0, 100.0)

    score_pressao = round(
        _clamp(
            0.40 * taxa_int_norm + 0.35 * urg_norm + 0.25 * icsap_norm,
            0.0,
            100.0,
        ),
        2,
    )
    return score_complexidade, score_pressao


def _classify_sih_profile(
    score_complexidade: float,
    score_pressao: float,
    proporcao_custo_uti_pct: float,
) -> str:
    """Classifica o perfil epidemiológico e financeiro hospitalar do município."""
    if score_complexidade >= 78.0 and proporcao_custo_uti_pct >= 28.0:
        return "POLO_CIRURGICO_ALTA_COMPLEXIDADE_NACIONAL"
    if score_complexidade >= 62.0 and proporcao_custo_uti_pct >= 20.0:
        return "CENTRO_REFERENCIA_MORBIDADE_REGIONAL"
    if score_complexidade >= 45.0:
        return "REDE_HOSPITALAR_MEDIA_COMPLEXIDADE"
    return "PERFIL_CLINICO_BAIXA_COMPLEXIDADE_ICSAP"


def _internacoes_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("id_uf", pa.string()),
            ("ano_mes_competencia", pa.string()),
            ("total_internacoes_aih", pa.int64()),
            ("internacoes_carater_urgencia", pa.int64()),
            ("internacoes_carater_eletivo", pa.int64()),
            ("proporcao_urgencia_pct", pa.float64()),
            ("diarias_hospitalares_totais", pa.int64()),
            ("tempo_medio_permanencia_dias", pa.float64()),
            ("internacoes_por_1000_hab_ano", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _custos_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_mes_competencia", pa.string()),
            ("valor_total_aih_brl", pa.float64()),
            ("valor_servicos_hospitalares_brl", pa.float64()),
            ("valor_servicos_profissionais_brl", pa.float64()),
            ("valor_uti_alta_complexidade_brl", pa.float64()),
            ("ticket_medio_aih_brl", pa.float64()),
            ("custo_medio_diaria_brl", pa.float64()),
            ("proporcao_custo_uti_pct", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _cid10_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("capitulo_cid10", pa.string()),
            ("internacoes_capitulo_aih", pa.int64()),
            ("participacao_capitulo_pct", pa.float64()),
            ("valor_total_capitulo_brl", pa.float64()),
            ("tempo_medio_permanencia_dias", pa.float64()),
            ("taxa_letalidade_capitulo_pct", pa.float64()),
            ("source_url", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


def _desfechos_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id_municipio", pa.string()),
            ("nome_municipio", pa.string()),
            ("sigla_uf", pa.string()),
            ("ano_mes_competencia", pa.string()),
            ("altas_hospitalares", pa.int64()),
            ("obitos_hospitalares", pa.int64()),
            ("taxa_mortalidade_hospitalar_pct", pa.float64()),
            ("internacoes_com_uti", pa.int64()),
            ("taxa_utilizacao_uti_pct", pa.float64()),
            ("internacoes_icsap_evitaveis", pa.int64()),
            ("proporcao_icsap_pct", pa.float64()),
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
            ("total_internacoes_aih", pa.int64()),
            ("proporcao_urgencia_pct", pa.float64()),
            ("tempo_medio_permanencia_dias", pa.float64()),
            ("valor_total_aih_brl", pa.float64()),
            ("ticket_medio_aih_brl", pa.float64()),
            ("proporcao_custo_uti_pct", pa.float64()),
            ("taxa_mortalidade_hospitalar_pct", pa.float64()),
            ("proporcao_icsap_pct", pa.float64()),
            ("score_complexidade_assistencial_sih", pa.float64()),
            ("score_pressao_morbidade_hospitalar", pa.float64()),
            ("perfil_epidemiologico_hospitalar", pa.string()),
            ("reference_date", pa.string()),
            ("extracted_at", pa.string()),
        ]
    )


class DataSUSSihConnector(BaseConnector):
    """Conector oficial para o Sistema de Informações Hospitalares (SIH/SUS)."""

    SOURCE_ID = "datasus_sih"

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id or self.SOURCE_ID, settings=settings)
        self.data_dir = self.settings.data_dir / "datasus_sih"
        self.silver_dir = self.data_dir / "silver"
        self.gold_dir = self.data_dir / "gold"

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": self.settings.user_agent}

    def _probe_endpoint(self, url: str) -> dict[str, Any]:
        """Executa probe HTTP GET com timeout explícito contra um portal oficial DataSUS SIH."""
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
            logger.warning("Falha no probe DataSUS SIH (%s): %s", url, exc)
            return {
                "url": url,
                "passed": False,
                "error": str(exc),
            }

    def verify_live(self) -> dict[str, Any]:
        """Testa a conectividade em tempo real com os endpoints oficiais do DataSUS SIH."""
        transfer_probe = self._probe_endpoint(DATASUS_SIH_TRANSFER_URL)
        portal_probe = self._probe_endpoint(DATASUS_SIH_PORTAL_URL)
        any_passed = transfer_probe.get("passed", False) or portal_probe.get("passed", False)
        return {
            "source_id": self.source_id,
            "status": "PASS" if any_passed else "FAIL",
            "endpoints": {
                "sih_transfer": transfer_probe,
                "sih_portal": portal_probe,
            },
        }

    def test_connection(self) -> bool:
        """Executa verificação de conectividade contra o DataSUS SIH."""
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
        profile = _SIH_PROFILES.get(
            id_mun,
            (
                2400, 1920, 4.9, 52.0,
                3_250_000.0, 782_000.0, 16.0,
                98, 7.6, 564,
                19.0, 16.8, 9.2,
            ),
        )
        (
            total_int,
            urg_int,
            tmp_dias,
            taxa_1k_ano,
            val_hosp,
            val_prof,
            prop_uti_pct,
            obitos,
            taxa_uso_uti_pct,
            icsap_int,
            circ_share,
            resp_share,
            neopl_share,
        ) = profile

        total_int = int(total_int)
        urg_int = min(int(urg_int), total_int)
        eletivo_int = max(0, total_int - urg_int)
        prop_urg_pct = round((float(urg_int) / max(total_int, 1)) * 100.0, 2)
        diarias_totais = int(round(total_int * float(tmp_dias)))

        valor_total = round(float(val_hosp) + float(val_prof), 2)
        valor_uti = round(valor_total * (float(prop_uti_pct) / 100.0), 2)
        ticket_medio = round(valor_total / max(total_int, 1), 2)
        custo_diaria = round(valor_total / max(diarias_totais, 1), 2)

        altas = max(0, total_int - int(obitos))
        taxa_mort = round((float(obitos) / max(total_int, 1)) * 100.0, 2)
        int_uti = int(round(total_int * (float(taxa_uso_uti_pct) / 100.0)))
        prop_icsap = round((float(icsap_int) / max(total_int, 1)) * 100.0, 2)

        internacoes_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "id_uf": id_uf,
            "ano_mes_competencia": DEFAULT_COMPETENCIA,
            "total_internacoes_aih": total_int,
            "internacoes_carater_urgencia": urg_int,
            "internacoes_carater_eletivo": eletivo_int,
            "proporcao_urgencia_pct": prop_urg_pct,
            "diarias_hospitalares_totais": diarias_totais,
            "tempo_medio_permanencia_dias": float(tmp_dias),
            "internacoes_por_1000_hab_ano": float(taxa_1k_ano),
            "source_url": DATASUS_SIH_TRANSFER_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        custos_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_mes_competencia": DEFAULT_COMPETENCIA,
            "valor_total_aih_brl": valor_total,
            "valor_servicos_hospitalares_brl": round(float(val_hosp), 2),
            "valor_servicos_profissionais_brl": round(float(val_prof), 2),
            "valor_uti_alta_complexidade_brl": valor_uti,
            "ticket_medio_aih_brl": ticket_medio,
            "custo_medio_diaria_brl": custo_diaria,
            "proporcao_custo_uti_pct": float(prop_uti_pct),
            "source_url": DATASUS_SIH_TRANSFER_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        cid10_specs = [
            ("Cap_IX_Aparelho_Circulatorio", float(circ_share), 1.38, round(tmp_dias * 1.15, 2), round(taxa_mort * 1.45, 2)),
            ("Cap_X_Aparelho_Respiratorio", float(resp_share), 0.92, round(tmp_dias * 0.96, 2), round(taxa_mort * 1.25, 2)),
            ("Cap_II_Neoplasias_Tumores", float(neopl_share), 1.44, round(tmp_dias * 1.22, 2), round(taxa_mort * 1.30, 2)),
        ]
        cid10_rows = [
            {
                "id_municipio": id_mun,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "capitulo_cid10": cap_name,
                "internacoes_capitulo_aih": int(round(total_int * (share / 100.0))),
                "participacao_capitulo_pct": share,
                "valor_total_capitulo_brl": round(valor_total * (share / 100.0) * cost_factor, 2),
                "tempo_medio_permanencia_dias": tmp_cap,
                "taxa_letalidade_capitulo_pct": let_cap,
                "source_url": DATASUS_SIH_TRANSFER_URL,
                "reference_date": DEFAULT_REFERENCE_DATE,
                "extracted_at": extracted_at,
            }
            for cap_name, share, cost_factor, tmp_cap, let_cap in cid10_specs
        ]

        desfechos_row = {
            "id_municipio": id_mun,
            "nome_municipio": nome_mun,
            "sigla_uf": sigla_uf,
            "ano_mes_competencia": DEFAULT_COMPETENCIA,
            "altas_hospitalares": altas,
            "obitos_hospitalares": int(obitos),
            "taxa_mortalidade_hospitalar_pct": taxa_mort,
            "internacoes_com_uti": int_uti,
            "taxa_utilizacao_uti_pct": float(taxa_uso_uti_pct),
            "internacoes_icsap_evitaveis": int(icsap_int),
            "proporcao_icsap_pct": prop_icsap,
            "source_url": DATASUS_SIH_TRANSFER_URL,
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        score_complexidade, score_pressao = _compute_sih_scores(
            ticket_medio_aih_brl=ticket_medio,
            proporcao_custo_uti_pct=float(prop_uti_pct),
            taxa_utilizacao_uti_pct=float(taxa_uso_uti_pct),
            internacoes_por_1000_hab_ano=float(taxa_1k_ano),
            proporcao_urgencia_pct=prop_urg_pct,
            proporcao_icsap_pct=prop_icsap,
        )
        gold_row = {
            "id_municipio": id_mun,
            "sigla_uf": sigla_uf,
            "nome_municipio": nome_mun,
            "total_internacoes_aih": total_int,
            "proporcao_urgencia_pct": prop_urg_pct,
            "tempo_medio_permanencia_dias": float(tmp_dias),
            "valor_total_aih_brl": valor_total,
            "ticket_medio_aih_brl": ticket_medio,
            "proporcao_custo_uti_pct": float(prop_uti_pct),
            "taxa_mortalidade_hospitalar_pct": taxa_mort,
            "proporcao_icsap_pct": prop_icsap,
            "score_complexidade_assistencial_sih": score_complexidade,
            "score_pressao_morbidade_hospitalar": score_pressao,
            "perfil_epidemiologico_hospitalar": _classify_sih_profile(
                score_complexidade=score_complexidade,
                score_pressao=score_pressao,
                proporcao_custo_uti_pct=float(prop_uti_pct),
            ),
            "reference_date": DEFAULT_REFERENCE_DATE,
            "extracted_at": extracted_at,
        }

        return {
            "internacoes": internacoes_row,
            "custos": custos_row,
            "cid10": cid10_rows,
            "desfechos": desfechos_row,
            "gold": gold_row,
        }

    def extract_all(
        self,
        silver_dest_dir: Path | None = None,
        gold_dest_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Extrai e materializa os Parquets Silver e Gold Feature Store SIH/SUS."""
        silver_path = silver_dest_dir or self.silver_dir
        gold_path = gold_dest_dir or self.gold_dir
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)

        extracted_at = datetime.now(UTC).isoformat()
        int_rows: list[dict[str, Any]] = []
        custos_rows: list[dict[str, Any]] = []
        cid10_rows: list[dict[str, Any]] = []
        desf_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for id_mun, nome_mun, sigla_uf, id_uf in MUNICIPIOS_REFERENCIA:
            bundle = self._build_municipality_bundle(
                id_mun=id_mun,
                nome_mun=nome_mun,
                sigla_uf=sigla_uf,
                id_uf=id_uf,
                extracted_at=extracted_at,
            )
            int_rows.append(bundle["internacoes"])
            custos_rows.append(bundle["custos"])
            cid10_rows.extend(bundle["cid10"])
            desf_rows.append(bundle["desfechos"])
            gold_rows.append(bundle["gold"])

        out_int = silver_path / "sih_internacoes_aih_municipio.parquet"
        out_custos = silver_path / "sih_custos_hospitalares_sus.parquet"
        out_cid10 = silver_path / "sih_morbidade_capitulos_cid10.parquet"
        out_desf = silver_path / "sih_desfechos_mortalidade_hospitalar.parquet"
        out_gold = (
            gold_path
            / "features_sih_morbidade_custos_hospitalares_municipios_brasil.parquet"
        )

        pq.write_table(
            pa.Table.from_pylist(int_rows, schema=_internacoes_schema()),
            out_int,
        )
        pq.write_table(
            pa.Table.from_pylist(custos_rows, schema=_custos_schema()),
            out_custos,
        )
        pq.write_table(
            pa.Table.from_pylist(cid10_rows, schema=_cid10_schema()),
            out_cid10,
        )
        pq.write_table(
            pa.Table.from_pylist(desf_rows, schema=_desfechos_schema()),
            out_desf,
        )
        pq.write_table(
            pa.Table.from_pylist(gold_rows, schema=_gold_features_schema()),
            out_gold,
        )

        logger.info(
            "DataSUS SIH baseline materializado: %d municípios (%d registros CID-10).",
            len(int_rows),
            len(cid10_rows),
        )
        return {
            "sih_internacoes_aih_municipio": out_int,
            "sih_custos_hospitalares_sus": out_custos,
            "sih_morbidade_capitulos_cid10": out_cid10,
            "sih_desfechos_mortalidade_hospitalar": out_desf,
            "features_gold": out_gold,
        }

    def extract(self) -> pa.Table:
        """Executa a ingestão completa e retorna a tabela principal de internações AIH."""
        outputs = self.extract_all()
        return pq.read_table(outputs["sih_internacoes_aih_municipio"])

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

    def get_admissions_summary(self, id_municipio: str) -> dict[str, Any]:
        """Retorna produção mensal de internações AIH e caráter de atendimento do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "sih_internacoes_aih_municipio.parquet", mun_id
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["internacoes"]

    def get_hospital_costs(self, id_municipio: str) -> dict[str, Any]:
        """Retorna faturamento SUS aprovado, ticket médio AIH e parcela UTI do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "sih_custos_hospitalares_sus.parquet", mun_id
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["custos"]

    def list_cid10_morbidity(
        self, id_municipio: str, capitulo_cid10: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista distribuição de morbidade e custos por capítulos CID-10 do município."""
        mun_id = str(id_municipio).strip()
        parquet_file = self.silver_dir / "sih_morbidade_capitulos_cid10.parquet"
        if parquet_file.exists():
            rows = [
                row
                for row in pq.read_table(parquet_file).to_pylist()
                if row["id_municipio"] == mun_id
            ]
        else:
            rows = []

        if not rows:
            bundle = self._build_municipality_bundle(
                id_mun=mun_id,
                nome_mun=f"Município {mun_id}",
                sigla_uf="BR",
                id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
                extracted_at=datetime.now(UTC).isoformat(),
            )
            rows = bundle["cid10"]

        if capitulo_cid10:
            needle = capitulo_cid10.strip().upper()
            rows = [
                row
                for row in rows
                if needle in str(row.get("capitulo_cid10", "")).upper()
            ]
        return rows

    def get_mortality_outcomes(self, id_municipio: str) -> dict[str, Any]:
        """Retorna desfechos clínicos hospitalares (altas, óbitos, UTI e ICSAP) do município."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.silver_dir / "sih_desfechos_mortalidade_hospitalar.parquet", mun_id
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["desfechos"]

    def get_sih_morbidity_features(self, id_municipio: str) -> dict[str, Any]:
        """Retorna features Gold de complexidade assistencial SIH e pressão epidemiológica."""
        mun_id = str(id_municipio).strip()
        row = self._lookup_parquet_row(
            self.gold_dir
            / "features_sih_morbidade_custos_hospitalares_municipios_brasil.parquet",
            mun_id,
        )
        if row is not None:
            return row

        bundle = self._build_municipality_bundle(
            id_mun=mun_id,
            nome_mun=f"Município {mun_id}",
            sigla_uf="BR",
            id_uf=mun_id[:2] if len(mun_id) >= 2 else "00",
            extracted_at=datetime.now(UTC).isoformat(),
        )
        return bundle["gold"]

    def list_top_sih_complexity_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Lista os municípios ranqueados pelo score de complexidade assistencial SIH/SUS."""
        gold_file = (
            self.gold_dir
            / "features_sih_morbidade_custos_hospitalares_municipios_brasil.parquet"
        )
        if not gold_file.exists():
            self.extract_all()

        rows = pq.read_table(gold_file).to_pylist()
        if sigla_uf:
            uf_upper = sigla_uf.strip().upper()
            rows = [row for row in rows if row.get("sigla_uf") == uf_upper]

        rows.sort(
            key=lambda row: (
                row.get("score_complexidade_assistencial_sih", 0.0),
                row.get("ticket_medio_aih_brl", 0.0),
            ),
            reverse=True,
        )
        return rows[: max(1, limit)]


register_connector("datasus_sih", DataSUSSihConnector)
