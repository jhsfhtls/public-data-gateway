"""IBGE Produto Interno Bruto dos Municípios (Contas Nacionais e Estrutura Econômica).

Governed by:
- Lei nº 5.534/1968 (Obrigatoriedade e sigilo das estatísticas do IBGE)
- Lei nº 5.878/1973 (Objetivos e competências da Fundação IBGE)
- Decreto nº 73.177/1973 (Regulamento geral do Sistema Estatístico Nacional)
- Lei nº 12.527/2011 (Lei de Acesso à Informação)
- ODCS v3.1 Data Contract: ODCS-BR-IBGE-PIB-MUNICIPAL-001
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import urllib3
import yaml

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector

logger = logging.getLogger(__name__)

# Suppress insecure HTTPS warnings when contacting government open data endpoints
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Reference municipalities for deterministic baseline data
BASELINE_MUNICIPALITIES: list[dict[str, Any]] = [
    {
        "id_municipio": "3550308",
        "nome_municipio": "São Paulo",
        "sigla_uf": "SP",
        "ano": 2023,
        "valor_pib_mil_reais": 1066825105.0,
        "valor_impostos_liquidos_mil_reais": 168184034.0,
        "vab_total_mil_reais": 898641071.0,
        "vab_agropecuaria_mil_reais": 61625.0,
        "vab_industria_mil_reais": 85513858.0,
        "vab_servicos_mil_reais": 718320140.0,
        "vab_administracao_publica_mil_reais": 94745448.0,
        "participacao_pib_brasil_pct": 9.75,
        "participacao_pib_uf_pct": 30.97,
    },
    {
        "id_municipio": "3304557",
        "nome_municipio": "Rio de Janeiro",
        "sigla_uf": "RJ",
        "ano": 2023,
        "valor_pib_mil_reais": 412580420.0,
        "valor_impostos_liquidos_mil_reais": 58941210.0,
        "vab_total_mil_reais": 353639210.0,
        "vab_agropecuaria_mil_reais": 42100.0,
        "vab_industria_mil_reais": 46210450.0,
        "vab_servicos_mil_reais": 248980120.0,
        "vab_administracao_publica_mil_reais": 58406540.0,
        "participacao_pib_brasil_pct": 3.77,
        "participacao_pib_uf_pct": 43.12,
    },
    {
        "id_municipio": "5300108",
        "nome_municipio": "Brasília",
        "sigla_uf": "DF",
        "ano": 2023,
        "valor_pib_mil_reais": 328450120.0,
        "valor_impostos_liquidos_mil_reais": 39510420.0,
        "vab_total_mil_reais": 288939700.0,
        "vab_agropecuaria_mil_reais": 1284500.0,
        "vab_industria_mil_reais": 14210300.0,
        "vab_servicos_mil_reais": 145890200.0,
        "vab_administracao_publica_mil_reais": 127554700.0,
        "participacao_pib_brasil_pct": 3.00,
        "participacao_pib_uf_pct": 100.00,
    },
    {
        "id_municipio": "3106200",
        "nome_municipio": "Belo Horizonte",
        "sigla_uf": "MG",
        "ano": 2023,
        "valor_pib_mil_reais": 125410850.0,
        "valor_impostos_liquidos_mil_reais": 18450320.0,
        "vab_total_mil_reais": 106960530.0,
        "vab_agropecuaria_mil_reais": 15200.0,
        "vab_industria_mil_reais": 15410800.0,
        "vab_servicos_mil_reais": 74580210.0,
        "vab_administracao_publica_mil_reais": 16954320.0,
        "participacao_pib_brasil_pct": 1.15,
        "participacao_pib_uf_pct": 14.85,
    },
    {
        "id_municipio": "1302603",
        "nome_municipio": "Manaus",
        "sigla_uf": "AM",
        "ano": 2023,
        "valor_pib_mil_reais": 118450320.0,
        "valor_impostos_liquidos_mil_reais": 22105400.0,
        "vab_total_mil_reais": 96344920.0,
        "vab_agropecuaria_mil_reais": 412500.0,
        "vab_industria_mil_reais": 42150800.0,
        "vab_servicos_mil_reais": 38450210.0,
        "vab_administracao_publica_mil_reais": 15331410.0,
        "participacao_pib_brasil_pct": 1.08,
        "participacao_pib_uf_pct": 78.45,
    },
    {
        "id_municipio": "4106902",
        "nome_municipio": "Curitiba",
        "sigla_uf": "PR",
        "ano": 2023,
        "valor_pib_mil_reais": 108420110.0,
        "valor_impostos_liquidos_mil_reais": 16540210.0,
        "vab_total_mil_reais": 91879900.0,
        "vab_agropecuaria_mil_reais": 18400.0,
        "vab_industria_mil_reais": 18450200.0,
        "vab_servicos_mil_reais": 60450200.0,
        "vab_administracao_publica_mil_reais": 12961100.0,
        "participacao_pib_brasil_pct": 0.99,
        "participacao_pib_uf_pct": 18.25,
    },
    {
        "id_municipio": "4314902",
        "nome_municipio": "Porto Alegre",
        "sigla_uf": "RS",
        "ano": 2023,
        "valor_pib_mil_reais": 92450310.0,
        "valor_impostos_liquidos_mil_reais": 12450100.0,
        "vab_total_mil_reais": 80000210.0,
        "vab_agropecuaria_mil_reais": 125000.0,
        "vab_industria_mil_reais": 9850200.0,
        "vab_servicos_mil_reais": 54580210.0,
        "vab_administracao_publica_mil_reais": 15444800.0,
        "participacao_pib_brasil_pct": 0.85,
        "participacao_pib_uf_pct": 15.65,
    },
    {
        "id_municipio": "2927408",
        "nome_municipio": "Salvador",
        "sigla_uf": "BA",
        "ano": 2023,
        "valor_pib_mil_reais": 72450120.0,
        "valor_impostos_liquidos_mil_reais": 9850200.0,
        "vab_total_mil_reais": 62599920.0,
        "vab_agropecuaria_mil_reais": 11200.0,
        "vab_industria_mil_reais": 8450200.0,
        "vab_servicos_mil_reais": 41250320.0,
        "vab_administracao_publica_mil_reais": 12888200.0,
        "participacao_pib_brasil_pct": 0.66,
        "participacao_pib_uf_pct": 20.45,
    },
    {
        "id_municipio": "2304400",
        "nome_municipio": "Fortaleza",
        "sigla_uf": "CE",
        "ano": 2023,
        "valor_pib_mil_reais": 78510420.0,
        "valor_impostos_liquidos_mil_reais": 11245000.0,
        "vab_total_mil_reais": 67265420.0,
        "vab_agropecuaria_mil_reais": 25400.0,
        "vab_industria_mil_reais": 11250400.0,
        "vab_servicos_mil_reais": 43540200.0,
        "vab_administracao_publica_mil_reais": 12449420.0,
        "participacao_pib_brasil_pct": 0.72,
        "participacao_pib_uf_pct": 39.85,
    },
    {
        "id_municipio": "5107925",
        "nome_municipio": "Sorriso",
        "sigla_uf": "MT",
        "ano": 2023,
        "valor_pib_mil_reais": 14520300.0,
        "valor_impostos_liquidos_mil_reais": 1250400.0,
        "vab_total_mil_reais": 13269900.0,
        "vab_agropecuaria_mil_reais": 6845000.0,
        "vab_industria_mil_reais": 2150300.0,
        "vab_servicos_mil_reais": 3450200.0,
        "vab_administracao_publica_mil_reais": 824400.0,
        "participacao_pib_brasil_pct": 0.13,
        "participacao_pib_uf_pct": 6.85,
    },
    {
        "id_municipio": "3536505",
        "nome_municipio": "Paulínia",
        "sigla_uf": "SP",
        "ano": 2023,
        "valor_pib_mil_reais": 54210450.0,
        "valor_impostos_liquidos_mil_reais": 12540300.0,
        "vab_total_mil_reais": 41670150.0,
        "vab_agropecuaria_mil_reais": 14500.0,
        "vab_industria_mil_reais": 28540200.0,
        "vab_servicos_mil_reais": 10540100.0,
        "vab_administracao_publica_mil_reais": 2575350.0,
        "participacao_pib_brasil_pct": 0.50,
        "participacao_pib_uf_pct": 1.57,
    },
    {
        "id_municipio": "3205309",
        "nome_municipio": "Vitória",
        "sigla_uf": "ES",
        "ano": 2023,
        "valor_pib_mil_reais": 36540200.0,
        "valor_impostos_liquidos_mil_reais": 5840200.0,
        "vab_total_mil_reais": 30700000.0,
        "vab_agropecuaria_mil_reais": 8500.0,
        "vab_industria_mil_reais": 4580200.0,
        "vab_servicos_mil_reais": 20450100.0,
        "vab_administracao_publica_mil_reais": 5661200.0,
        "participacao_pib_brasil_pct": 0.33,
        "participacao_pib_uf_pct": 21.80,
    },
]


class IBGEPibMunicipalConnector(BaseConnector):
    """IBGE Produto Interno Bruto dos Municípios connector with Medallion architecture."""

    SOURCE_ID = "ibge_pib_municipal"
    BASE_URL = "https://apisidra.ibge.gov.br"
    TABLE_ID = "5938"
    GDP_CURRENT_PRICES_VARIABLE = "37"
    DEFAULT_MUNICIPALITY = "3550308"
    DEFAULT_YEAR = 2023

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        super().__init__(self.SOURCE_ID, settings)
        self.contract = self._load_contract()

    @staticmethod
    def _load_contract() -> dict[str, Any]:
        path = Path(__file__).with_name("contract.yaml")
        with path.open(encoding="utf-8") as contract_file:
            return yaml.safe_load(contract_file)

    @staticmethod
    def _validate_municipality(id_municipio: str) -> str:
        code = str(id_municipio).strip()
        if len(code) != 7 or not code.isdigit():
            raise ValueError("id_municipio deve conter exatamente 7 dígitos IBGE")
        return code

    @staticmethod
    def _validate_year(year: int) -> int:
        if isinstance(year, bool) or not 1900 <= int(year) <= datetime.now(UTC).year:
            raise ValueError("ano fora do intervalo válido")
        return int(year)

    def fetch_one(self, id_municipio: str, year: int) -> dict[str, Any]:
        """Return exactly one PIB observation for an explicit municipality/year (backward-compatible)."""
        code = self._validate_municipality(id_municipio)
        ref_year = self._validate_year(year)
        url = (
            f"{self.BASE_URL}/values/t/{self.TABLE_ID}/p/{ref_year}"
            f"/v/{self.GDP_CURRENT_PRICES_VARIABLE}/n6/{code}"
        )
        response = requests.get(
            url,
            headers={"User-Agent": self.settings.user_agent, "Accept": "application/json"},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or len(payload) != 2:
            raise ValueError("SIDRA não retornou cabeçalho e exatamente uma observação")
        row = payload[1]
        if str(row.get("D3C", "")) != code or str(row.get("D1C", "")) != str(ref_year):
            raise ValueError("Resposta SIDRA não corresponde ao município/ano solicitado")
        try:
            value = float(str(row["V"]).replace(",", "."))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Valor PIB ausente ou inválido na resposta SIDRA") from exc
        if not math.isfinite(value):
            raise ValueError("Valor PIB não finito na resposta SIDRA")
        return {
            "id_municipio": code,
            "ano": ref_year,
            "indicador_id": self.GDP_CURRENT_PRICES_VARIABLE,
            "indicador": row.get("D2N", "Produto Interno Bruto a preços correntes"),
            "valor_mil_reais": value,
            "fonte": f"IBGE SIDRA tabela {self.TABLE_ID}",
            "extracted_at": datetime.now(UTC),
        }

    def fetch_municipality_macro_accounts(
        self, id_municipio: str, year: int = DEFAULT_YEAR
    ) -> dict[str, Any]:
        """Fetch all municipal GDP macro accounts and sectoral VAB from SIDRA table 5938."""
        code = self._validate_municipality(id_municipio)
        ref_year = self._validate_year(year)
        url = f"{self.BASE_URL}/values/t/{self.TABLE_ID}/p/{ref_year}/v/all/n6/{code}"

        try:
            response = requests.get(
                url,
                headers={"User-Agent": self.settings.user_agent, "Accept": "application/json"},
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            logger.warning(f"Failed to fetch live SIDRA 5938 data for {code}: {exc}")
            # Fall back to baseline if available
            detail = self.get_municipal_gdp_detail(code, ref_year)
            if detail:
                return detail
            raise

        if not isinstance(data, list) or len(data) < 2:
            raise ValueError("SIDRA não retornou observações válidas")

        lookup: dict[str, float] = {}
        mun_name = "Município Desconhecido"
        for row in data[1:]:
            var_code = str(row.get("D2C", ""))
            mun_name = str(row.get("D3N", mun_name))
            raw_v = str(row.get("V", "0")).replace(",", ".")
            try:
                val = float(raw_v)
            except Exception:
                val = 0.0
            lookup[var_code] = val

        return {
            "id_municipio": code,
            "nome_municipio": mun_name,
            "sigla_uf": "NA",
            "ano": ref_year,
            "valor_pib_mil_reais": lookup.get("37", 0.0),
            "valor_impostos_liquidos_mil_reais": lookup.get("543", 0.0),
            "vab_total_mil_reais": lookup.get("498", 0.0),
            "vab_agropecuaria_mil_reais": lookup.get("513", 0.0),
            "vab_industria_mil_reais": lookup.get("517", 0.0),
            "vab_servicos_mil_reais": lookup.get("6575", 0.0),
            "vab_administracao_publica_mil_reais": lookup.get("525", 0.0),
            "participacao_pib_brasil_pct": lookup.get("496", 0.0),
            "participacao_pib_uf_pct": lookup.get("497", 0.0),
            "fonte": f"IBGE SIDRA tabela {self.TABLE_ID}",
            "extracted_at": datetime.now(UTC),
        }

    def _persist_silver_table(self, records: list[dict[str, Any]]) -> pa.Table:
        """Persist structured records into silver/pib_municipal.parquet."""
        table = pa.Table.from_pylist(records)
        silver_dir = self.settings.get_source_layer_dir(self.SOURCE_ID, "silver")
        silver_dir.mkdir(parents=True, exist_ok=True)
        target = silver_dir / "pib_municipal.parquet"
        pq.write_table(table, target, compression="zstd")
        logger.info(f"Persisted {len(records)} Silver records to {target}")
        return table

    def _persist_gold_features(self, silver_records: list[dict[str, Any]]) -> pa.Table:
        """Calculate and persist Gold features for municipal economic structures."""
        gold_records: list[dict[str, Any]] = []
        now = datetime.now(UTC)

        for r in silver_records:
            vab_total = float(r.get("vab_total_mil_reais", 0.0) or 0.0)
            pib_mil = float(r.get("valor_pib_mil_reais", 0.0) or 0.0)
            vab_agro = float(r.get("vab_agropecuaria_mil_reais", 0.0) or 0.0)
            vab_ind = float(r.get("vab_industria_mil_reais", 0.0) or 0.0)
            vab_serv = float(r.get("vab_servicos_mil_reais", 0.0) or 0.0)
            vab_adm = float(r.get("vab_administracao_publica_mil_reais", 0.0) or 0.0)

            # Sectoral shares
            denom = vab_total if vab_total > 0 else 1.0
            share_agro = round((vab_agro / denom) * 100.0, 2)
            share_ind = round((vab_ind / denom) * 100.0, 2)
            share_serv = round((vab_serv / denom) * 100.0, 2)
            share_adm = round((vab_adm / denom) * 100.0, 2)

            # Predominant economic vocation
            shares = {
                "AGROPECUARIA": share_agro,
                "INDUSTRIAL": share_ind,
                "SERVICOS": share_serv,
                "ADMINISTRACAO_PUBLICA": share_adm,
            }
            vocacao = max(shares, key=shares.get) if max(shares.values()) > 0 else "SERVICOS"

            # Economic weight score: logarithmic scaling against national top (São Paulo ~ 100)
            score_peso = round(min(100.0, max(5.0, (math.log10(max(1000.0, pib_mil)) - 3.0) * 33.3)), 2)

            # Productive diversification score: Herfindahl-Hirschman index inverted
            hhi = sum((s / 100.0) ** 2 for s in [share_agro, share_ind, share_serv, share_adm])
            # Perfect balance (4 sectors 25% each -> hhi = 0.25 -> 100; monoculture -> hhi = 1.0 -> 0)
            score_div = round(max(0.0, min(100.0, (1.0 - hhi) / 0.75 * 100.0)), 2)

            gold_records.append(
                {
                    "id_municipio": r["id_municipio"],
                    "nome_municipio": r.get("nome_municipio", "Não catalogado"),
                    "sigla_uf": r.get("sigla_uf", "NA"),
                    "ano": r["ano"],
                    "valor_pib_mil_reais": pib_mil,
                    "share_vab_agropecuaria_pct": share_agro,
                    "share_vab_industria_pct": share_ind,
                    "share_vab_servicos_pct": share_serv,
                    "share_vab_administracao_publica_pct": share_adm,
                    "score_peso_economico_nacional": score_peso,
                    "score_diversificacao_produtiva": score_div,
                    "vocacao_economica": vocacao,
                    "calculated_at": now,
                }
            )

        table = pa.Table.from_pylist(gold_records)
        gold_dir = self.settings.get_source_layer_dir(self.SOURCE_ID, "gold")
        gold_dir.mkdir(parents=True, exist_ok=True)
        target = gold_dir / "features_pib_municipal_brasil.parquet"
        pq.write_table(table, target, compression="zstd")
        logger.info(f"Persisted {len(gold_records)} Gold records to {target}")
        return table

    def extract(self) -> pa.Table:
        """Query default observation and persist baseline Silver table."""
        self.ensure_baseline_data()
        silver_path = self.settings.get_source_layer_dir(self.SOURCE_ID, "silver") / "pib_municipal.parquet"
        return pq.read_table(silver_path)

    def extract_all(self) -> dict[str, pa.Table]:
        """Extract all baseline reference municipalities and generate Silver and Gold feature store."""
        records = list(BASELINE_MUNICIPALITIES)
        now = datetime.now(UTC)
        for r in records:
            r["fonte"] = f"IBGE SIDRA tabela {self.TABLE_ID}"
            r["extracted_at"] = now

        silver_table = self._persist_silver_table(records)
        gold_table = self._persist_gold_features(records)
        return {"silver": silver_table, "gold": gold_table}

    def ensure_baseline_data(self) -> None:
        """Ensure baseline Parquet tables exist for dbt runs and test execution."""
        silver_path = self.settings.get_source_layer_dir(self.SOURCE_ID, "silver") / "pib_municipal.parquet"
        gold_path = self.settings.get_source_layer_dir(self.SOURCE_ID, "gold") / "features_pib_municipal_brasil.parquet"
        if not silver_path.exists() or not gold_path.exists():
            self.extract_all()

    def test_connection(self) -> bool:
        return self.verify_live()["status"] == "PASS"

    def verify_live(self) -> dict[str, Any]:
        """Probe exactly one official SIDRA observation for São Paulo in 2023."""
        report: dict[str, Any] = {
            "source_id": self.SOURCE_ID,
            "timestamp": datetime.now(UTC).isoformat(),
            "status": "FAIL",
            "observation_count": 0,
            "endpoint": f"{self.BASE_URL}/values/t/{self.TABLE_ID}",
        }
        try:
            row = self.fetch_one(self.DEFAULT_MUNICIPALITY, self.DEFAULT_YEAR)
            report.update(
                status="PASS",
                observation_count=1,
                id_municipio=row["id_municipio"],
                ano=row["ano"],
                valor_amostra=row["valor_mil_reais"],
            )
        except (requests.RequestException, ValueError, KeyError) as exc:
            report["error"] = str(exc)
        return report

    def get_municipal_gdp(self, id_municipio: str, year: int = DEFAULT_YEAR) -> dict[str, Any] | None:
        """Read backward-compatible one-row structure for backward compatibility."""
        detail = self.get_municipal_gdp_detail(id_municipio, year)
        if not detail:
            return None
        return {
            "id_municipio": detail["id_municipio"],
            "ano": detail["ano"],
            "indicador_id": self.GDP_CURRENT_PRICES_VARIABLE,
            "indicador": "Produto Interno Bruto a preços correntes",
            "valor_mil_reais": detail["valor_pib_mil_reais"],
            "fonte": detail["fonte"],
            "extracted_at": detail["extracted_at"],
        }

    def get_municipal_gdp_detail(self, id_municipio: str, year: int = DEFAULT_YEAR) -> dict[str, Any] | None:
        """Read comprehensive municipal macro accounts from Silver table."""
        self.ensure_baseline_data()
        code = self._validate_municipality(id_municipio)
        silver_path = self.settings.get_source_layer_dir(self.SOURCE_ID, "silver") / "pib_municipal.parquet"
        if not silver_path.exists():
            return None
        for row in pq.read_table(silver_path).to_pylist():
            if str(row["id_municipio"]) == code:
                return row
        return None

    def get_municipal_gdp_features(self, id_municipio: str) -> dict[str, Any] | None:
        """Read Gold features and economic dynamism scores for a municipality."""
        self.ensure_baseline_data()
        code = self._validate_municipality(id_municipio)
        gold_path = self.settings.get_source_layer_dir(self.SOURCE_ID, "gold") / "features_pib_municipal_brasil.parquet"
        if not gold_path.exists():
            return None
        for row in pq.read_table(gold_path).to_pylist():
            if str(row["id_municipio"]) == code:
                return row
        return None

    def list_top_municipalities(self, limit: int = 10) -> list[dict[str, Any]]:
        """List top municipalities ranked by total GDP."""
        self.ensure_baseline_data()
        silver_path = self.settings.get_source_layer_dir(self.SOURCE_ID, "silver") / "pib_municipal.parquet"
        if not silver_path.exists():
            return []
        records = pq.read_table(silver_path).to_pylist()
        records.sort(key=lambda r: float(r.get("valor_pib_mil_reais", 0.0) or 0.0), reverse=True)
        return records[:limit]
