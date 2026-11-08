"""Unit and offline integration test suite for IBGE SIDRA connector.

Tests:
1. ODCS v3.1 Data Contract conformance
2. Ingestion and Silver Parquet materialization for all 5 tables
3. Gold Feature Store generation (features_socioeconomico_municipios_brasil.parquet)
4. Municipal GDP indicators query by 7-digit IBGE code
5. Socioeconomic profile and per capita GDP calculation
6. IPCA inflation series query
7. Gini index and household income query by State (UF)
8. Multi-interface parity: SDK, CLI, REST API, and MCP
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.demografico.ibge_sidra import (
    IBGESidraConnector,
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
def sidra_connector(temp_gateway_settings: GatewaySettings) -> IBGESidraConnector:
    """Instantiate IBGESidraConnector backed by temporary storage."""
    return IBGESidraConnector(settings=temp_gateway_settings)


def test_sidra_odcs_contract(sidra_connector: IBGESidraConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = sidra_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-IBGE-SIDRA-001"
    assert contract.source_id == "ibge_sidra"
    assert "id_municipio" in contract.primary_keys
    assert "ano" in contract.primary_keys
    assert contract.min_expected_rows >= 5


def test_sidra_extract_all_tables(sidra_connector: IBGESidraConnector) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    tables = sidra_connector.extract_all()
    assert len(tables) == 5
    assert "sidra_pib_municipal" in tables
    assert "sidra_populacao_censo" in tables
    assert "sidra_agropecuaria_pam" in tables
    assert "sidra_ipca_mensal" in tables
    assert "sidra_gini_rendimento_uf" in tables

    for name, table in tables.items():
        assert table.num_rows > 0, f"Table {name} is empty"

    # Check primary table columns
    cols = tables["sidra_pib_municipal"].column_names
    assert "id_municipio" in cols
    assert "nome_municipio" in cols
    assert "sigla_uf" in cols
    assert "ano" in cols
    assert "pib_mil_reais" in cols
    assert "vab_total_mil_reais" in cols
    assert "vab_agropecuaria_mil_reais" in cols
    assert "vab_industria_mil_reais" in cols
    assert "vab_servicos_mil_reais" in cols
    assert "vab_administracao_publica_mil_reais" in cols
    assert "participacao_pib_brasil_pct" in cols


def test_sidra_gold_feature_store(sidra_connector: IBGESidraConnector) -> None:
    """Validate Gold feature store calculation and schema."""
    sidra_connector.ensure_baseline_data()
    gold_path = (
        sidra_connector.settings.data_dir
        / sidra_connector.SOURCE_ID
        / "gold"
        / "features_socioeconomico_municipios_brasil.parquet"
    )
    assert gold_path.exists(), "Gold feature store parquet was not generated"

    table = pq.read_table(gold_path)
    assert table.num_rows > 0
    cols = table.column_names
    assert "id_municipio" in cols
    assert "nome_municipio" in cols
    assert "sigla_uf" in cols
    assert "pib_per_capita_reais" in cols
    assert "perfil_economico_dominante" in cols
    assert "classificacao_porte_municipio" in cols
    assert "score_dinamismo_economico" in cols
    assert "indice_gini_uf" in cols
    assert "ipca_acumulado_12m_pct" in cols

    records = table.to_pylist()
    sp = next((r for r in records if r["id_municipio"] == "3550308"), None)
    assert sp is not None
    assert sp["perfil_economico_dominante"] == "SERVICOS"
    assert sp["classificacao_porte_municipio"] == "METROPOLE"
    assert sp["pib_per_capita_reais"] > 50000.0

    sorriso = next((r for r in records if r["id_municipio"] == "5107925"), None)
    assert sorriso is not None
    assert sorriso["perfil_economico_dominante"] == "AGROPECUARIO"


def test_sidra_get_municipality_gdp(sidra_connector: IBGESidraConnector) -> None:
    """Validate retrieval of GDP indicators by 7-digit IBGE code."""
    sidra_connector.ensure_baseline_data()
    res = sidra_connector.get_municipality_gdp("3550308")
    assert res is not None
    assert res["nome_municipio"] == "São Paulo"
    assert res["sigla_uf"] == "SP"
    assert res["pib_mil_reais"] > 1000000000.0
    assert res["vab_servicos_mil_reais"] > 500000000.0

    not_found = sidra_connector.get_municipality_gdp("9999999")
    assert not_found is None


def test_sidra_get_socioeconomic_profile(sidra_connector: IBGESidraConnector) -> None:
    """Validate retrieval of Gold socioeconomic profile."""
    sidra_connector.ensure_baseline_data()
    prof = sidra_connector.get_municipality_socioeconomic_profile("1302603")
    assert prof is not None
    assert prof["nome_municipio"] == "Manaus"
    assert prof["sigla_uf"] == "AM"
    assert prof["perfil_economico_dominante"] in ("INDUSTRIAL", "SERVICOS")
    assert prof["populacao_residente"] > 2000000
    assert prof["score_dinamismo_economico"] > 0.0


def test_sidra_ipca_history(sidra_connector: IBGESidraConnector) -> None:
    """Validate retrieval of IPCA inflation history."""
    sidra_connector.ensure_baseline_data()
    history = sidra_connector.get_ipca_history(limit=5)
    assert len(history) == 5
    first = history[0]
    assert "ano_mes" in first
    assert "ipca_variacao_mensal_pct" in first
    assert "ipca_acumulado_12m_pct" in first


def test_sidra_gini_uf(sidra_connector: IBGESidraConnector) -> None:
    """Validate retrieval of Gini index by State (UF)."""
    sidra_connector.ensure_baseline_data()
    gini_sp = sidra_connector.get_gini_uf("SP")
    assert gini_sp is not None
    assert gini_sp["nome_uf"] == "São Paulo"
    assert 0.40 <= gini_sp["indice_gini_rendimento"] <= 0.60
    assert gini_sp["rendimento_medio_domiciliar_reais"] > 2000.0

    not_found = sidra_connector.get_gini_uf("XX")
    assert not_found is None


def test_sidra_top_municipalities(sidra_connector: IBGESidraConnector) -> None:
    """Validate listing top municipalities ranked by GDP."""
    sidra_connector.ensure_baseline_data()
    top = sidra_connector.list_top_municipalities_by_gdp(limit=3)
    assert len(top) == 3
    # Top 1 should be São Paulo
    assert top[0]["id_municipio"] == "3550308"


def test_sidra_multi_interface_parity(temp_gateway_settings: GatewaySettings) -> None:
    """Validate consistent access across SDK, REST API, and MCP dispatcher."""
    # 1. SDK Facade
    sdk = BrazilData(settings=temp_gateway_settings)
    gdp_sdk = sdk.get_municipality_gdp("3550308")
    assert gdp_sdk is not None
    assert gdp_sdk["nome_municipio"] == "São Paulo"

    prof_sdk = sdk.socioeconomic.profile("3550308")
    assert prof_sdk is not None
    assert prof_sdk["perfil_economico_dominante"] == "SERVICOS"

    # 2. REST API
    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    r_gdp = client.get("/api/v1/socioeconomic/municipality/3550308/gdp")
    assert r_gdp.status_code == 200
    assert r_gdp.json()["nome_municipio"] == "São Paulo"

    r_prof = client.get("/api/v1/socioeconomic/municipality/3550308/profile")
    assert r_prof.status_code == 200
    assert r_prof.json()["perfil_economico_dominante"] == "SERVICOS"

    r_ipca = client.get("/api/v1/socioeconomic/ipca?limit=3")
    assert r_ipca.status_code == 200
    assert len(r_ipca.json()) == 3

    r_gini = client.get("/api/v1/socioeconomic/gini/SP")
    assert r_gini.status_code == 200
    assert r_gini.json()["sigla_uf"] == "SP"

    # 3. MCP Dispatcher with Prompt Injection Protection
    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req_mcp = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_municipality_socioeconomic_profile",
            "arguments": {"id_municipio": "3550308"},
        },
    }
    res_mcp = dispatcher.handle_request(req_mcp)
    assert not res_mcp.get("error")
    content = res_mcp["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in content
    assert "São Paulo" in content
