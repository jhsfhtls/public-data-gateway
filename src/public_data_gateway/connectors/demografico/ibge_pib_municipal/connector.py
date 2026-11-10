"""Controlled access to one observation from IBGE SIDRA table 5938.

The bulk publication is intentionally not downloaded by this connector. Every
network query requires an explicit municipality and year and SIDRA is asked
for one variable, which keeps live probes and interactive use bounded to one
observation.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector


class IBGEPibMunicipalConnector(BaseConnector):
    """Fetch one selected municipal GDP value without bulk extraction."""

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
        """Return exactly one PIB observation for an explicit municipality/year."""
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

    def extract(self) -> pa.Table:
        """Query one bounded default observation and persist it as Silver."""
        row = self.fetch_one(self.DEFAULT_MUNICIPALITY, self.DEFAULT_YEAR)
        table = pa.Table.from_pylist([row])
        target = self.settings.get_source_layer_dir(self.SOURCE_ID, "silver") / "pib_municipal.parquet"
        pq.write_table(table, target, compression="zstd")
        return table

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

    def get_municipal_gdp(self, id_municipio: str, year: int) -> dict[str, Any] | None:
        """Read an observation from the locally materialized one-row Silver file."""
        code = self._validate_municipality(id_municipio)
        ref_year = self._validate_year(year)
        path = self.settings.get_source_layer_dir(self.SOURCE_ID, "silver") / "pib_municipal.parquet"
        if not path.exists():
            return None
        for row in pq.read_table(path).to_pylist():
            if row["id_municipio"] == code and row["ano"] == ref_year:
                return row
        return None
