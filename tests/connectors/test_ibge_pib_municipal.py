"""Unit and integration tests for IBGE municipal GDP connector and interfaces."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.demografico.ibge_pib_municipal import (
    IBGEPibMunicipalConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture
def connector(tmp_path: Path) -> IBGEPibMunicipalConnector:
    settings = GatewaySettings(repo_root=tmp_path, data_dir=tmp_path / "data")
    settings.ensure_directories()
    conn = IBGEPibMunicipalConnector(settings=settings)
    conn.extract_all()
    return conn


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
    assert contract["data_governance"]["grain"] == "municipio_ano"
    assert contract["data_governance"]["primary_keys"] == ["id_municipio", "ano"]
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


def test_extract_persists_silver_and_gold_tables(connector: IBGEPibMunicipalConnector) -> None:
    result = connector.extract()
    silver_target = connector.settings.data_dir / connector.SOURCE_ID / "silver" / "pib_municipal.parquet"
    gold_target = connector.settings.data_dir / connector.SOURCE_ID / "gold" / "features_pib_municipal_brasil.parquet"

    assert result.num_rows >= 10
    assert pq.read_table(silver_target).num_rows >= 10
    assert pq.read_table(gold_target).num_rows >= 10


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


def test_get_municipal_gdp_detail_reads_baseline(connector: IBGEPibMunicipalConnector) -> None:
    sp = connector.get_municipal_gdp_detail("3550308")
    assert sp is not None
    assert sp["nome_municipio"] == "São Paulo"
    assert sp["sigla_uf"] == "SP"
    assert sp["valor_pib_mil_reais"] > 1_000_000_000.0
    assert sp["vab_servicos_mil_reais"] > 0
    assert sp["participacao_pib_brasil_pct"] > 9.0


def test_get_municipal_gdp_features_returns_gold_scores(connector: IBGEPibMunicipalConnector) -> None:
    sp_feat = connector.get_municipal_gdp_features("3550308")
    assert sp_feat is not None
    assert sp_feat["score_peso_economico_nacional"] > 90.0
    assert 0.0 <= sp_feat["score_diversificacao_produtiva"] <= 100.0
    assert sp_feat["vocacao_economica"] == "SERVICOS"

    sorriso_feat = connector.get_municipal_gdp_features("5107925")
    assert sorriso_feat is not None
    assert sorriso_feat["vocacao_economica"] == "AGROPECUARIA"


def test_list_top_municipalities_order(connector: IBGEPibMunicipalConnector) -> None:
    top = connector.list_top_municipalities(limit=5)
    assert len(top) == 5
    assert top[0]["id_municipio"] == "3550308"  # São Paulo is #1
    for i in range(len(top) - 1):
        assert top[i]["valor_pib_mil_reais"] >= top[i + 1]["valor_pib_mil_reais"]


def test_sdk_parity_municipal_gdp(connector: IBGEPibMunicipalConnector) -> None:
    sdk = BrazilData(settings=connector.settings)
    sp_detail = sdk.get_municipal_gdp_detail("3550308")
    assert sp_detail is not None
    assert sp_detail["valor_pib_mil_reais"] > 0

    sp_features = sdk.get_municipal_gdp_features("3550308")
    assert sp_features is not None
    assert sp_features["score_peso_economico_nacional"] > 0

    top_pib = sdk.list_top_municipal_gdp(limit=3)
    assert len(top_pib) == 3


def test_api_parity_municipal_gdp(connector: IBGEPibMunicipalConnector) -> None:
    app = create_app(settings=connector.settings)
    client = TestClient(app)
    resp = client.get("/api/v1/demographics/pib-municipal/3550308")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id_municipio"] == "3550308"
    assert data["valor_pib_mil_reais"] > 0

    resp_short = client.get("/api/v1/pib-municipal/3550308")
    assert resp_short.status_code == 200

    resp_feat = client.get("/api/v1/demographics/pib-municipal/features/3550308")
    assert resp_feat.status_code == 200
    feat_data = resp_feat.json()
    assert feat_data["vocacao_economica"] == "SERVICOS"

    resp_top = client.get("/api/v1/demographics/pib-municipal/top?limit=3")
    assert resp_top.status_code == 200
    assert len(resp_top.json()) == 3


def test_mcp_parity_municipal_gdp(connector: IBGEPibMunicipalConnector) -> None:
    server = MCPDispatcher(settings=connector.settings)
    req = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "tools/call",
        "params": {
            "name": "bdg_get_pib_municipal_detail",
            "arguments": {"id_municipio": "3550308"},
        },
    }
    resp = server.handle_request(req)
    assert resp["id"] == "1"
    content = resp["result"]["content"][0]["text"]
    assert "3550308" in content
    assert "untrusted_gov_data" in content
