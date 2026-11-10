"""Offline tests for the row-bounded IBGE municipal GDP connector."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from public_data_gateway.connectors.demografico.ibge_pib_municipal import (
    IBGEPibMunicipalConnector,
)
from public_data_gateway.core.config import GatewaySettings


@pytest.fixture
def connector(tmp_path: Path) -> IBGEPibMunicipalConnector:
    settings = GatewaySettings(repo_root=tmp_path, data_dir=tmp_path / "data")
    settings.ensure_directories()
    return IBGEPibMunicipalConnector(settings=settings)


class StubResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    @staticmethod
    def json() -> list[dict[str, str]]:
        return [
            {"D1C": "2023", "D2C": "37", "D3C": "3550308"},
            {
                "D1C": "2023",
                "D2C": "37",
                "D2N": "Produto Interno Bruto a preços correntes",
                "D3C": "3550308",
                "V": "1066825105",
            },
        ]


def test_odcs_contract_declares_grain_keys_sla_and_lgpd(connector: IBGEPibMunicipalConnector) -> None:
    contract = connector.contract
    assert contract["schema_version"] == "3.1.0"
    assert contract["contract_id"] == "ODCS-BR-IBGE-PIB-MUNICIPAL-001"
    assert contract["data_governance"]["grain"] == "municipio_ano_indicador"
    assert contract["data_governance"]["primary_keys"] == ["id_municipio", "ano", "indicador_id"]
    assert contract["data_governance"]["freshness_sla_hours"] > 0
    assert contract["data_governance"]["lgpd_tier"] == "PUBLIC_NON_PERSONAL"


def test_fetch_one_parses_one_row_and_builds_sidra_request(
    connector: IBGEPibMunicipalConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, Any] = {}

    def fake_get(url: str, **kwargs: Any) -> StubResponse:
        seen.update(url=url, **kwargs)
        return StubResponse()

    monkeypatch.setattr("public_data_gateway.connectors.demografico.ibge_pib_municipal.connector.requests.get", fake_get)
    row = connector.fetch_one("3550308", 2023)
    assert row["id_municipio"] == "3550308"
    assert row["ano"] == 2023
    assert row["valor_mil_reais"] == 1066825105.0
    assert "/t/5938/p/2023/v/37/n6/3550308" in seen["url"]
    assert seen["timeout"] == 15


def test_extract_persists_only_one_observation_in_silver(
    connector: IBGEPibMunicipalConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        connector,
        "fetch_one",
        lambda municipality, year: {
            "id_municipio": municipality,
            "ano": year,
            "indicador_id": "37",
            "indicador": "PIB corrente",
            "valor_mil_reais": 42.0,
            "fonte": "IBGE SIDRA tabela 5938",
            "extracted_at": datetime.now(UTC),
        },
    )
    result = connector.extract()
    target = connector.settings.data_dir / connector.SOURCE_ID / "silver" / "pib_municipal.parquet"
    assert result.num_rows == 1
    assert pq.read_table(target).num_rows == 1
    assert connector.get_municipal_gdp("3550308", 2023)["valor_mil_reais"] == 42.0
    assert connector.get_municipal_gdp("3304557", 2023) is None


@pytest.mark.parametrize("code", ["123", "12345678", "abcdefg"])
def test_fetch_one_rejects_invalid_municipality_without_network(
    connector: IBGEPibMunicipalConnector, code: str
) -> None:
    with pytest.raises(ValueError, match="7 dígitos"):
        connector.fetch_one(code, 2023)


def test_fetch_one_rejects_response_with_more_than_one_observation(
    connector: IBGEPibMunicipalConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ManyRows(StubResponse):
        @staticmethod
        def json() -> list[dict[str, str]]:
            return StubResponse.json() + [StubResponse.json()[1]]

    monkeypatch.setattr(
        "public_data_gateway.connectors.demografico.ibge_pib_municipal.connector.requests.get",
        lambda *args, **kwargs: ManyRows(),
    )
    with pytest.raises(ValueError, match="exatamente uma observação"):
        connector.fetch_one("3550308", 2023)
