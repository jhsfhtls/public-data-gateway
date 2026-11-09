"""Unit and offline integration test suite for IBGE PNAD Contínua connector.

Tests:
1. ODCS v3.1 Data Contract conformance
2. Ingestion and Silver Parquet materialization for all 5 tables
3. Gold Feature Store generation (features_pnad_trabalho_renda_estados_brasil.parquet)
4. Quarterly labor force indicators query by UF
5. Gold labor market profile and resilience score calculation
6. Real earnings summary query
7. State rankings by unemployment and earnings
8. Multi-interface parity: SDK, REST API, and MCP
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.trabalho.ibge_pnad_continua import (
    IBGEPnadContinuaConnector,
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
def pnad_connector(temp_gateway_settings: GatewaySettings) -> IBGEPnadContinuaConnector:
    """Instantiate IBGEPnadContinuaConnector backed by temporary storage."""
    return IBGEPnadContinuaConnector(settings=temp_gateway_settings)


def test_pnad_odcs_contract(pnad_connector: IBGEPnadContinuaConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = pnad_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-IBGE-PNADCONT-001"
    assert contract.source_id == "ibge_pnad_continua"
    assert "id_uf" in contract.primary_keys
    assert "ano_trimestre" in contract.primary_keys
    assert contract.min_expected_rows >= 5


def test_pnad_extract_all_tables(pnad_connector: IBGEPnadContinuaConnector) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    tables = pnad_connector.extract_all()
    assert len(tables) == 5
    assert "pnad_mercado_trabalho_trimestral" in tables
    assert "pnad_rendimento_trabalho" in tables
    assert "pnad_subutilizacao_forca" in tables
    assert "pnad_posicao_ocupacao" in tables
    assert "pnad_demografia_educacao_trabalho" in tables

    for name, table in tables.items():
        assert table.num_rows == 27, f"Table {name} does not have all 27 UFs"

    # Check primary table columns
    cols = tables["pnad_mercado_trabalho_trimestral"].column_names
    assert "id_uf" in cols
    assert "sigla_uf" in cols
    assert "nome_uf" in cols
    assert "ano_trimestre" in cols
    assert "populacao_idade_trabalhar_mil" in cols
    assert "forca_trabalho_total_mil" in cols
    assert "populacao_ocupada_mil" in cols
    assert "populacao_desocupada_mil" in cols
    assert "taxa_desocupacao_pct" in cols
    assert "taxa_participacao_forca_pct" in cols
    assert "taxa_nivel_ocupacao_pct" in cols


def test_pnad_gold_feature_store(pnad_connector: IBGEPnadContinuaConnector) -> None:
    """Validate Gold feature store calculation and schema."""
    pnad_connector.ensure_baseline_data()
    gold_path = (
        pnad_connector.settings.data_dir
        / pnad_connector.SOURCE_ID
        / "gold"
        / "features_pnad_trabalho_renda_estados_brasil.parquet"
    )
    assert gold_path.exists(), "Gold feature store parquet was not generated"

    table = pq.read_table(gold_path)
    assert table.num_rows == 27
    cols = table.column_names
    assert "id_uf" in cols
    assert "sigla_uf" in cols
    assert "nome_uf" in cols
    assert "ano_trimestre_recente" in cols
    assert "taxa_desocupacao_pct" in cols
    assert "rendimento_medio_real_habitual_reais" in cols
    assert "score_resiliencia_mercado_trabalho" in cols
    assert "score_vulnerabilidade_trabalho_renda" in cols
    assert "classificacao_mercado_trabalho_uf" in cols

    records = table.to_pylist()
    sp = next((r for r in records if r["sigla_uf"] == "SP"), None)
    assert sp is not None
    assert sp["score_resiliencia_mercado_trabalho"] > 70.0
    assert sp["rendimento_medio_real_habitual_reais"] > 4000.0


def test_pnad_get_labor_force_indicators(pnad_connector: IBGEPnadContinuaConnector) -> None:
    """Validate retrieval of labor force indicators by UF code or sigla."""
    pnad_connector.ensure_baseline_data()
    res_sp = pnad_connector.get_labor_force_indicators("SP")
    assert res_sp is not None
    assert res_sp["nome_uf"] == "São Paulo"
    assert res_sp["id_uf"] == "35"
    assert res_sp["taxa_desocupacao_pct"] == 6.2

    res_35 = pnad_connector.get_labor_force_indicators("35")
    assert res_35 is not None
    assert res_35["sigla_uf"] == "SP"

    not_found = pnad_connector.get_labor_force_indicators("XX")
    assert not_found is None


def test_pnad_get_state_labor_profile(pnad_connector: IBGEPnadContinuaConnector) -> None:
    """Validate retrieval of Gold labor profile."""
    pnad_connector.ensure_baseline_data()
    prof = pnad_connector.get_state_labor_profile("AM")
    assert prof is not None
    assert prof["nome_uf"] == "Amazonas"
    assert prof["taxa_informalidade_pct"] > 50.0
    assert prof["score_resiliencia_mercado_trabalho"] > 0.0

    not_found = pnad_connector.get_state_labor_profile("99")
    assert not_found is None


def test_pnad_get_earnings_summary(pnad_connector: IBGEPnadContinuaConnector) -> None:
    """Validate retrieval of real earnings summary."""
    pnad_connector.ensure_baseline_data()
    earn = pnad_connector.get_earnings_summary("DF")
    assert earn is not None
    assert earn["nome_uf"] == "Distrito Federal"
    assert earn["rendimento_medio_real_habitual_reais"] > 5000.0

    not_found = pnad_connector.get_earnings_summary("99")
    assert not_found is None


def test_pnad_list_rankings(pnad_connector: IBGEPnadContinuaConnector) -> None:
    """Validate ranking of states by unemployment rate and real earnings."""
    pnad_connector.ensure_baseline_data()
    # Lowest unemployment (Santa Catarina should be among the top)
    lowest_unemp = pnad_connector.list_lowest_unemployment_states(limit=3)
    assert len(lowest_unemp) == 3
    assert lowest_unemp[0]["taxa_desocupacao_pct"] <= lowest_unemp[1]["taxa_desocupacao_pct"]

    # Highest earnings (Distrito Federal should be first)
    highest_earn = pnad_connector.list_highest_earnings_states(limit=3)
    assert len(highest_earn) == 3
    assert highest_earn[0]["rendimento_medio_real_habitual_reais"] >= highest_earn[1]["rendimento_medio_real_habitual_reais"]
    assert highest_earn[0]["sigla_uf"] == "DF"


def test_pnad_multi_interface_parity(temp_gateway_settings: GatewaySettings) -> None:
    """Validate consistent access across SDK, REST API, and MCP dispatcher."""
    # 1. SDK Facade
    sdk = BrazilData(settings=temp_gateway_settings)
    ind_sdk = sdk.labor.force_indicators("SP")
    assert ind_sdk is not None
    assert ind_sdk["taxa_desocupacao_pct"] == 6.2

    prof_sdk = sdk.get_state_labor_profile("SP")
    assert prof_sdk is not None
    assert prof_sdk["score_resiliencia_mercado_trabalho"] > 70.0

    unemp_sdk = sdk.get_lowest_unemployment_states(limit=2)
    assert len(unemp_sdk) == 2

    # 2. REST API
    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    r_ind = client.get("/api/v1/labor/state/SP/indicators")
    assert r_ind.status_code == 200
    assert r_ind.json()["sigla_uf"] == "SP"

    r_prof = client.get("/api/v1/labor/state/SP/profile")
    assert r_prof.status_code == 200
    assert r_prof.json()["score_resiliencia_mercado_trabalho"] > 70.0

    r_short = client.get("/api/v1/pnad/SP")
    assert r_short.status_code == 200
    assert r_short.json()["sigla_uf"] == "SP"

    r_earn = client.get("/api/v1/labor/state/DF/earnings")
    assert r_earn.status_code == 200
    assert r_earn.json()["rendimento_medio_real_habitual_reais"] > 5000.0

    r_rank_u = client.get("/api/v1/labor/ranking/unemployment?limit=3")
    assert r_rank_u.status_code == 200
    assert len(r_rank_u.json()) == 3

    r_rank_e = client.get("/api/v1/labor/ranking/earnings?limit=3")
    assert r_rank_e.status_code == 200
    assert len(r_rank_e.json()) == 3

    # 3. MCP Dispatcher with Untrusted Gov Data Containment
    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req_mcp = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_state_labor_profile",
            "arguments": {"uf": "SP"},
        },
    }
    res_mcp = dispatcher.handle_request(req_mcp)
    assert not res_mcp.get("error")
    content = res_mcp["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in content
    assert "ibge_pnad_continua" in content
    assert "São Paulo" in content
