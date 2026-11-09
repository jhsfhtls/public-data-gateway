"""Unit and offline integration test suite for IBGE CNEFE connector.

Tests:
1. ODCS v3.1 Data Contract conformance
2. Ingestion and Silver Parquet materialization for all 5 tables
3. Gold Feature Store generation (features_cnefe_enderecamento_municipios_brasil.parquet)
4. Municipality address summary query by IBGE code or name
5. CEP profile and centroid coordinate query
6. Building species breakdown by municipality
7. Address density rankings across municipalities
8. Multi-interface parity: SDK, REST API, and MCP dispatcher
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.geografico.ibge_cnefe import (
    IBGECnefeConnector,
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
def cnefe_connector(temp_gateway_settings: GatewaySettings) -> IBGECnefeConnector:
    """Instantiate IBGECnefeConnector backed by temporary storage."""
    return IBGECnefeConnector(settings=temp_gateway_settings)


def test_cnefe_odcs_contract(cnefe_connector: IBGECnefeConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = cnefe_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-IBGE-CNEFE-001"
    assert contract.source_id == "ibge_cnefe"
    assert "id_municipio" in contract.primary_keys
    assert "cep_8" in contract.primary_keys
    assert contract.min_expected_rows >= 5


def test_cnefe_extract_all_tables(cnefe_connector: IBGECnefeConnector) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    tables = cnefe_connector.extract_all()
    assert len(tables) == 6
    assert "cnefe_enderecos_padrao" in tables
    assert "cnefe_coordenadas_geograficas" in tables
    assert "cnefe_tipos_edificacao" in tables
    assert "cnefe_agregados_cep" in tables
    assert "cnefe_densidade_enderecamento" in tables
    assert "features_cnefe_enderecamento_municipios_brasil" in tables

    for name, table in tables.items():
        assert table.num_rows >= 27, f"Table {name} has fewer than 27 rows"

    # Check primary table columns
    cols = tables["cnefe_enderecos_padrao"].column_names
    assert "id_municipio" in cols
    assert "sigla_uf" in cols
    assert "nome_municipio" in cols
    assert "codigo_distrito" in cols
    assert "codigo_setor_censitario" in cols
    assert "seq_endereco" in cols
    assert "cep_8" in cols
    assert "tipo_logradouro" in cols
    assert "nome_logradouro" in cols
    assert "numero_predial" in cols
    assert "especie_edificacao" in cols


def test_cnefe_gold_feature_store(cnefe_connector: IBGECnefeConnector) -> None:
    """Validate Gold feature store calculation and schema."""
    cnefe_connector.ensure_baseline_data()
    gold_path = (
        cnefe_connector.settings.data_dir
        / cnefe_connector.SOURCE_ID
        / "gold"
        / "features_cnefe_enderecamento_municipios_brasil.parquet"
    )
    assert gold_path.exists(), "Gold feature store parquet was not generated"

    table = pq.read_table(gold_path)
    assert table.num_rows == 27
    cols = table.column_names
    assert "id_municipio" in cols
    assert "nome_municipio" in cols
    assert "sigla_uf" in cols
    assert "total_enderecos" in cols
    assert "total_domicilios_particulares" in cols
    assert "total_estabelecimentos_comerciais_servicos" in cols
    assert "score_densidade_comercial" in cols
    assert "score_infraestrutura_social" in cols
    assert "taxa_cobertura_georreferenciada_pct" in cols

    records = table.to_pylist()
    sp = next((r for r in records if r["id_municipio"] == "3550308"), None)
    assert sp is not None
    assert sp["total_enderecos"] == 65000
    assert sp["total_domicilios_particulares"] > 50000
    assert sp["taxa_cobertura_georreferenciada_pct"] > 99.0


def test_cnefe_get_municipality_summary(cnefe_connector: IBGECnefeConnector) -> None:
    """Validate retrieval of address and building summary by municipality code or name."""
    cnefe_connector.ensure_baseline_data()
    res_code = cnefe_connector.get_municipality_summary("3550308")
    assert res_code is not None
    assert res_code["nome_municipio"] == "São Paulo"
    assert res_code["sigla_uf"] == "SP"
    assert res_code["total_enderecos"] == 65000

    res_name = cnefe_connector.get_municipality_summary("são paulo")
    assert res_name is not None
    assert res_name["id_municipio"] == "3550308"

    not_found = cnefe_connector.get_municipality_summary("9999999")
    assert not_found is None


def test_cnefe_get_cep_profile(cnefe_connector: IBGECnefeConnector) -> None:
    """Validate retrieval of CEP profile with centroid coordinates and counts."""
    cnefe_connector.ensure_baseline_data()
    profile = cnefe_connector.get_cep_profile("35503010")
    assert profile is not None
    assert profile["cep_8"] == "35503010"
    assert profile["id_municipio"] == "3550308"
    assert profile["total_enderecos"] == 4500
    assert profile["total_domicilios"] == 2925
    assert profile["total_estabelecimentos"] == 1575
    assert profile["latitude_centroide"] < 0.0
    assert profile["longitude_centroide"] < 0.0

    not_found = cnefe_connector.get_cep_profile("00000000")
    assert not_found is None


def test_cnefe_list_building_types(cnefe_connector: IBGECnefeConnector) -> None:
    """Validate listing of building species breakdown."""
    cnefe_connector.ensure_baseline_data()
    types_list = cnefe_connector.list_building_types("3550308")
    assert len(types_list) == 8
    # Residential should be the largest category
    assert types_list[0]["tipo_edificacao"] == "Domicílio particular"
    assert types_list[0]["participacao_pct"] > 80.0

    not_found = cnefe_connector.list_building_types("0000000")
    assert len(not_found) == 0


def test_cnefe_list_top_address_density(cnefe_connector: IBGECnefeConnector) -> None:
    """Validate ranking of municipalities by address density per km2."""
    cnefe_connector.ensure_baseline_data()
    ranks = cnefe_connector.list_top_address_density(limit=3)
    assert len(ranks) == 3
    assert ranks[0]["densidade_enderecos_km2"] >= ranks[1]["densidade_enderecos_km2"]
    assert ranks[1]["densidade_enderecos_km2"] >= ranks[2]["densidade_enderecos_km2"]


def test_cnefe_multi_interface_parity(temp_gateway_settings: GatewaySettings) -> None:
    """Validate consistent access across SDK, REST API, and MCP dispatcher."""
    # 1. SDK Facade
    sdk = BrazilData(settings=temp_gateway_settings)
    sum_sdk = sdk.get_cnefe_municipality_summary("3550308")
    assert sum_sdk is not None
    assert sum_sdk["total_enderecos"] == 65000

    cep_sdk = sdk.get_cnefe_cep_profile("35503010")
    assert cep_sdk is not None
    assert cep_sdk["total_enderecos"] == 4500

    types_sdk = sdk.get_cnefe_building_types("3550308")
    assert len(types_sdk) == 8

    ranks_sdk = sdk.get_cnefe_top_address_density(limit=2)
    assert len(ranks_sdk) == 2

    # 2. REST API
    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    r_sum = client.get("/api/v1/geo/cnefe/municipality/3550308/summary")
    assert r_sum.status_code == 200
    assert r_sum.json()["total_enderecos"] == 65000

    r_short = client.get("/api/v1/cnefe/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["sigla_uf"] == "SP"

    r_cep = client.get("/api/v1/geo/cnefe/cep/35503010")
    assert r_cep.status_code == 200
    assert r_cep.json()["total_enderecos"] == 4500

    r_types = client.get("/api/v1/geo/cnefe/municipality/3550308/building-types")
    assert r_types.status_code == 200
    assert len(r_types.json()) == 8

    r_ranks = client.get("/api/v1/geo/cnefe/ranking/density?limit=3")
    assert r_ranks.status_code == 200
    assert len(r_ranks.json()) == 3

    # 3. MCP Dispatcher with Untrusted Gov Data Containment
    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req_mcp = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_cnefe_municipality_summary",
            "arguments": {"municipio": "3550308"},
        },
    }
    res_mcp = dispatcher.handle_request(req_mcp)
    assert not res_mcp.get("error")
    content = res_mcp["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in content
    assert "ibge_cnefe" in content
    assert "3550308" in content
