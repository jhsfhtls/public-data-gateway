"""Unit and offline integration test suite for MTE Novo CAGED connector.

Tests:
1. ODCS v3.1 Data Contract conformance
2. Ingestion and Silver Parquet materialization for all 5 tables
3. Gold Feature Store generation (features_mercado_trabalho_municipios_brasil.parquet)
4. Employment summary query by 7-digit IBGE code
5. Gold labor market profile and heating score calculation
6. Sectoral employment dynamics query
7. Top job-creating municipalities ranking
8. Multi-interface parity: SDK, REST API, and MCP
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.trabalho.mte_novo_caged import (
    MTENovoCagedConnector,
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
def caged_connector(temp_gateway_settings: GatewaySettings) -> MTENovoCagedConnector:
    """Instantiate MTENovoCagedConnector backed by temporary storage."""
    return MTENovoCagedConnector(settings=temp_gateway_settings)


def test_caged_odcs_contract(caged_connector: MTENovoCagedConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = caged_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-MTE-NOVOCAGED-001"
    assert contract.source_id == "mte_novo_caged"
    assert "id_municipio" in contract.primary_keys
    assert "competencia_movimentacao" in contract.primary_keys
    assert contract.min_expected_rows >= 5


def test_caged_extract_all_tables(caged_connector: MTENovoCagedConnector) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    tables = caged_connector.extract_all()
    assert len(tables) == 5
    assert "caged_movimentacao_mensal" in tables
    assert "caged_perfil_setorial_cnae" in tables
    assert "caged_ocupacao_cbo" in tables
    assert "caged_demografia_trabalhador" in tables
    assert "caged_salarios_remuneracao" in tables

    for name, table in tables.items():
        assert table.num_rows > 0, f"Table {name} is empty"

    # Check primary table columns
    cols = tables["caged_movimentacao_mensal"].column_names
    assert "id_municipio" in cols
    assert "nome_municipio" in cols
    assert "sigla_uf" in cols
    assert "competencia_movimentacao" in cols
    assert "total_admissoes" in cols
    assert "total_desligamentos" in cols
    assert "saldo_movimentacao" in cols
    assert "salario_medio_admissao_reais" in cols
    assert "taxa_rotatividade_turnover_pct" in cols


def test_caged_gold_feature_store(caged_connector: MTENovoCagedConnector) -> None:
    """Validate Gold feature store calculation and schema."""
    caged_connector.ensure_baseline_data()
    gold_path = (
        caged_connector.settings.data_dir
        / caged_connector.SOURCE_ID
        / "gold"
        / "features_mercado_trabalho_municipios_brasil.parquet"
    )
    assert gold_path.exists(), "Gold feature store parquet was not generated"

    table = pq.read_table(gold_path)
    assert table.num_rows > 0
    cols = table.column_names
    assert "id_municipio" in cols
    assert "nome_municipio" in cols
    assert "sigla_uf" in cols
    assert "competencia_recente" in cols
    assert "saldo_liquido_emprego_ano" in cols
    assert "estoque_declarado_aproximado" in cols
    assert "score_aquecimento_mercado_trabalho" in cols
    assert "score_vulnerabilidade_emprego" in cols
    assert "classificacao_dinamica_emprego" in cols

    records = table.to_pylist()
    sp = next((r for r in records if r["id_municipio"] == "3550308"), None)
    assert sp is not None
    assert sp["saldo_liquido_emprego_ano"] > 0
    assert sp["score_aquecimento_mercado_trabalho"] > 70.0


def test_caged_get_employment_summary(caged_connector: MTENovoCagedConnector) -> None:
    """Validate retrieval of monthly employment summary by 7-digit IBGE code."""
    caged_connector.ensure_baseline_data()
    res = caged_connector.get_employment_summary("3550308")
    assert res is not None
    assert res["nome_municipio"] == "São Paulo"
    assert res["sigla_uf"] == "SP"
    assert res["saldo_movimentacao"] > 0
    assert res["total_admissoes"] > res["total_desligamentos"]

    not_found = caged_connector.get_employment_summary("9999999")
    assert not_found is None


def test_caged_get_labor_market_profile(caged_connector: MTENovoCagedConnector) -> None:
    """Validate retrieval of Gold labor market profile."""
    caged_connector.ensure_baseline_data()
    prof = caged_connector.get_labor_market_profile("1302603")
    assert prof is not None
    assert prof["nome_municipio"] == "Manaus"
    assert prof["sigla_uf"] == "AM"
    assert prof["score_aquecimento_mercado_trabalho"] > 0.0
    assert prof["score_vulnerabilidade_emprego"] > 0.0
    assert prof["setor_lider_geracao_emprego"] in ("INDUSTRIA_GERAL", "SERVICOS", "COMERCIO")

    not_found = caged_connector.get_labor_market_profile("9999999")
    assert not_found is None


def test_caged_get_sectoral_employment(caged_connector: MTENovoCagedConnector) -> None:
    """Validate retrieval of employment movements across CNAE sectors."""
    caged_connector.ensure_baseline_data()
    sectors = caged_connector.get_sectoral_employment("3550308")
    assert len(sectors) > 0
    sector_names = [s["grande_setor_cnae"] for s in sectors]
    assert "SERVICOS" in sector_names

    not_found = caged_connector.get_sectoral_employment("9999999")
    assert not_found == []


def test_caged_list_top_job_creating_municipalities(caged_connector: MTENovoCagedConnector) -> None:
    """Validate ranking of top job-creating municipalities."""
    caged_connector.ensure_baseline_data()
    ranks = caged_connector.list_top_job_creating_municipalities(limit=3)
    assert len(ranks) == 3
    assert ranks[0]["saldo_movimentacao"] >= ranks[1]["saldo_movimentacao"]


def test_caged_multi_interface_parity(temp_gateway_settings: GatewaySettings) -> None:
    """Validate consistent access across SDK, REST API, and MCP dispatcher."""
    # 1. SDK Facade
    sdk = BrazilData(settings=temp_gateway_settings)
    summary_sdk = sdk.labor.employment_summary("3550308")
    assert summary_sdk is not None
    assert summary_sdk["nome_municipio"] == "São Paulo"

    prof_sdk = sdk.get_labor_market_profile("3550308")
    assert prof_sdk is not None
    assert prof_sdk["score_aquecimento_mercado_trabalho"] > 70.0

    top_sdk = sdk.labor.top_job_creators(limit=2)
    assert len(top_sdk) == 2

    # 2. REST API
    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    r_sum = client.get("/api/v1/labor/municipality/3550308/summary")
    assert r_sum.status_code == 200
    assert r_sum.json()["nome_municipio"] == "São Paulo"

    r_prof = client.get("/api/v1/labor/municipality/3550308/profile")
    assert r_prof.status_code == 200
    assert r_prof.json()["score_aquecimento_mercado_trabalho"] > 70.0

    r_short = client.get("/api/v1/caged/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["nome_municipio"] == "São Paulo"

    r_sec = client.get("/api/v1/labor/municipality/3550308/sectors")
    assert r_sec.status_code == 200
    assert len(r_sec.json()) > 0

    r_rank = client.get("/api/v1/labor/ranking?limit=3")
    assert r_rank.status_code == 200
    assert len(r_rank.json()) == 3

    # 3. MCP Dispatcher with Untrusted Gov Data Containment
    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req_mcp = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_labor_market_profile",
            "arguments": {"id_municipio": "3550308"},
        },
    }
    res_mcp = dispatcher.handle_request(req_mcp)
    assert not res_mcp.get("error")
    content = res_mcp["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in content
    assert "mte_novo_caged" in content
    assert "São Paulo" in content
