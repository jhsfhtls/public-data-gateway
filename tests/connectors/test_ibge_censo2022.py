"""Unit and offline integration test suite for IBGE Censo Demográfico 2022 connector.

Tests:
1. ODCS v3.1 Data Contract conformance
2. Ingestion and Silver Parquet materialization for all 5 tables
3. Gold Feature Store generation (features_censo2022_municipios_brasil.parquet)
4. Census summary query by 7-digit IBGE code
5. Gold demographic profile and indicators calculation
6. Literacy rates and indicators query
7. Highest aging municipalities listing
8. Multi-interface parity: SDK, REST API, and MCP
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.demografico.ibge_censo2022 import (
    IBGECenso2022Connector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture
def temp_gateway_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    """Create isolated temporary directory environment."""
    monkeypatch.setenv("BDG_OFFLINE_MODE", "1")
    repo_root = Path(__file__).resolve().parents[2]
    settings = GatewaySettings(
        repo_root=repo_root,
        data_dir=tmp_path / "data",
    )
    settings.ensure_directories()
    return settings


@pytest.fixture
def censo_connector(temp_gateway_settings: GatewaySettings) -> IBGECenso2022Connector:
    """Instantiate IBGECenso2022Connector backed by temporary storage."""
    return IBGECenso2022Connector(settings=temp_gateway_settings)


def test_censo_odcs_contract(censo_connector: IBGECenso2022Connector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = censo_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-IBGE-CENSO2022-001"
    assert contract.source_id == "ibge_censo2022"
    assert "id_municipio" in contract.primary_keys
    assert "ano_censo" in contract.primary_keys
    assert contract.min_expected_rows >= 5


def test_censo_extract_all_tables(censo_connector: IBGECenso2022Connector) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    tables = censo_connector.extract_all()
    assert len(tables) == 5
    assert "censo_populacao_domicilios" in tables
    assert "censo_estrutura_etaria" in tables
    assert "censo_educacao_alfabetizacao" in tables
    assert "censo_urbanizacao_situacao" in tables
    assert "censo_diversidade_quilombola_indigena" in tables

    for name, table in tables.items():
        assert table.num_rows > 0, f"Table {name} is empty"

    # Check primary table columns
    cols = tables["censo_populacao_domicilios"].column_names
    assert "id_municipio" in cols
    assert "nome_municipio" in cols
    assert "sigla_uf" in cols
    assert "ano_censo" in cols
    assert "populacao_residente" in cols
    assert "total_domicilios_particulares_ocupados" in cols
    assert "media_moradores_por_domicilio" in cols
    assert "densidade_demografica_hab_km2" in cols


def test_censo_gold_feature_store(censo_connector: IBGECenso2022Connector) -> None:
    """Validate Gold feature store calculation and schema."""
    censo_connector.ensure_baseline_data()
    gold_path = (
        censo_connector.settings.data_dir
        / censo_connector.SOURCE_ID
        / "gold"
        / "features_censo2022_municipios_brasil.parquet"
    )
    assert gold_path.exists(), "Gold feature store parquet was not generated"

    table = pq.read_table(gold_path)
    assert table.num_rows > 0
    cols = table.column_names
    assert "id_municipio" in cols
    assert "nome_municipio" in cols
    assert "sigla_uf" in cols
    assert "populacao_residente" in cols
    assert "idade_mediana_anos" in cols
    assert "indice_envelhecimento_razao" in cols
    assert "classificacao_etaria" in cols
    assert "taxa_alfabetizacao_pct" in cols
    assert "taxa_urbanizacao_pct" in cols
    assert "score_maturidade_social" in cols
    assert "score_vulnerabilidade_demografica" in cols

    records = table.to_pylist()
    sp = next((r for r in records if r["id_municipio"] == "3550308"), None)
    assert sp is not None
    assert sp["classificacao_densidade"] in ("HIPERDENSA", "ALTA_DENSIDADE")
    assert sp["populacao_residente"] > 10000000
    assert sp["score_maturidade_social"] > 90.0


def test_censo_get_summary(censo_connector: IBGECenso2022Connector) -> None:
    """Validate retrieval of census summary counts by 7-digit IBGE code."""
    censo_connector.ensure_baseline_data()
    res = censo_connector.get_census_summary("3550308")
    assert res is not None
    assert res["nome_municipio"] == "São Paulo"
    assert res["sigla_uf"] == "SP"
    assert res["populacao_residente"] == 11451999
    assert res["total_domicilios_particulares_ocupados"] == 4307665

    not_found = censo_connector.get_census_summary("9999999")
    assert not_found is None


def test_censo_get_demographic_profile(censo_connector: IBGECenso2022Connector) -> None:
    """Validate retrieval of Gold demographic profile."""
    censo_connector.ensure_baseline_data()
    prof = censo_connector.get_demographic_profile("1302603")
    assert prof is not None
    assert prof["nome_municipio"] == "Manaus"
    assert prof["sigla_uf"] == "AM"
    assert prof["classificacao_etaria"] == "JOVEM"
    assert prof["populacao_indigena"] > 50000
    assert prof["score_maturidade_social"] > 0.0


def test_censo_get_literacy_indicators(censo_connector: IBGECenso2022Connector) -> None:
    """Validate retrieval of literacy indicators."""
    censo_connector.ensure_baseline_data()
    data = censo_connector.get_literacy_indicators("3550308")
    assert data is not None
    assert data["taxa_alfabetizacao_pct"] > 95.0
    assert data["populacao_alfabetizada"] > 9000000

    not_found = censo_connector.get_literacy_indicators("9999999")
    assert not_found is None


def test_censo_list_highest_aging_municipalities(censo_connector: IBGECenso2022Connector) -> None:
    """Validate ranking of municipalities with highest aging index."""
    censo_connector.ensure_baseline_data()
    ranks = censo_connector.list_highest_aging_municipalities(limit=3)
    assert len(ranks) == 3
    # First should have highest aging ratio
    assert ranks[0]["indice_envelhecimento_razao"] >= ranks[1]["indice_envelhecimento_razao"]


def test_censo_multi_interface_parity(temp_gateway_settings: GatewaySettings) -> None:
    """Validate consistent access across SDK, REST API, and MCP dispatcher."""
    # 1. SDK Facade
    sdk = BrazilData(settings=temp_gateway_settings)
    summary_sdk = sdk.get_census_summary("3550308")
    assert summary_sdk is not None
    assert summary_sdk["nome_municipio"] == "São Paulo"

    prof_sdk = sdk.socioeconomic.demographic_profile("3550308")
    assert prof_sdk is not None
    assert prof_sdk["score_maturidade_social"] > 90.0

    # 2. REST API
    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    r_sum = client.get("/api/v1/census/municipality/3550308/summary")
    assert r_sum.status_code == 200
    assert r_sum.json()["nome_municipio"] == "São Paulo"

    r_prof = client.get("/api/v1/census/municipality/3550308/profile")
    assert r_prof.status_code == 200
    assert r_prof.json()["classificacao_densidade"] in ("HIPERDENSA", "ALTA_DENSIDADE")

    r_lit = client.get("/api/v1/census/municipality/3550308/literacy")
    assert r_lit.status_code == 200
    assert r_lit.json()["taxa_alfabetizacao_pct"] > 95.0

    r_age = client.get("/api/v1/census/aging/highest?limit=3")
    assert r_age.status_code == 200
    assert len(r_age.json()) == 3

    # 3. MCP Dispatcher with Prompt Injection Protection
    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req_mcp = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_demographic_profile",
            "arguments": {"id_municipio": "3550308"},
        },
    }
    res_mcp = dispatcher.handle_request(req_mcp)
    assert not res_mcp.get("error")
    content = res_mcp["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in content
    assert "São Paulo" in content
