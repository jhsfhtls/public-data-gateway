"""Unit and offline integration test suite for Compras.gov.br CATMAT/CATSER connector.

Tests:
1. ODCS v3.1 Data Contract conformance
2. Ingestion and Silver Parquet materialization for all 5 tables
3. Gold Feature Store generation (features_compras_catalogo_brasil.parquet)
4. Material search and catalog detail lookup
5. Service search query
6. Procurement price benchmark, dispersion and overpricing risk
7. Supplier procurement history query
8. Multi-interface parity: SDK, CLI, REST API, and MCP
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.compras_publicas.comprasgov_catmat import (
    ComprasGovCatmatConnector,
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
def catmat_connector(temp_gateway_settings: GatewaySettings) -> ComprasGovCatmatConnector:
    """Instantiate ComprasGovCatmatConnector backed by temporary storage."""
    return ComprasGovCatmatConnector(settings=temp_gateway_settings)


def test_catmat_odcs_contract(catmat_connector: ComprasGovCatmatConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = catmat_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-MGI-CATMAT-001"
    assert contract.source_id == "comprasgov_catmat"
    assert "codigo_item" in contract.primary_keys
    assert contract.min_expected_rows >= 10


def test_catmat_extract_all_tables(catmat_connector: ComprasGovCatmatConnector) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    tables = catmat_connector.extract_all()
    assert len(tables) == 5
    assert "catmat_itens_material" in tables
    assert "catser_itens_servico" in tables
    assert "catmat_grupos_classes" in tables
    assert "catmat_precos_praticados" in tables
    assert "catmat_fornecedores_compras" in tables

    for name, table in tables.items():
        assert table.num_rows > 0, f"Table {name} is empty"

    # Check primary table columns
    cols = tables["catmat_itens_material"].column_names
    assert "codigo_item" in cols
    assert "codigo_pdm" in cols
    assert "nome_pdm" in cols
    assert "descricao_item" in cols
    assert "item_sustentavel" in cols


def test_catmat_gold_feature_store(
    catmat_connector: ComprasGovCatmatConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Validate Gold Feature Store metrics, competitiveness and overpricing risk ratings."""
    catmat_connector.extract_all()
    gold_path = (
        temp_gateway_settings.data_dir
        / "comprasgov_catmat"
        / "gold"
        / "features_compras_catalogo_brasil.parquet"
    )
    assert gold_path.exists(), "Gold feature file not found"

    table = pq.read_table(gold_path)
    assert table.num_rows > 0

    cols = table.column_names
    assert "codigo_item_ou_pdm" in cols
    assert "tipo_item" in cols
    assert "preco_medio_praticado" in cols
    assert "desvio_padrao_preco" in cols
    assert "coeficiente_variacao_preco_pct" in cols
    assert "indice_competitividade_fornecedores" in cols
    assert "rating_risco_sobrepreco" in cols


def test_catmat_search_and_detail(catmat_connector: ComprasGovCatmatConnector) -> None:
    """Validate material search and unique item lookup."""
    results = catmat_connector.search_materials("CADEIRA")
    assert len(results) > 0
    assert any("CADEIRA" in r["nome_pdm"].upper() for r in results)

    # Item detail
    item = catmat_connector.get_item_detail("206504")
    assert item is not None
    assert item["codigo_pdm"] == "00313"
    assert "ESCRITORIO" in item["nome_pdm"]

    # Non-existent item
    missing = catmat_connector.get_item_detail("999999999")
    assert missing is None


def test_catser_services_search(catmat_connector: ComprasGovCatmatConnector) -> None:
    """Validate CATSER service searching."""
    servs = catmat_connector.search_services("SOFTWARE")
    assert len(servs) > 0
    assert any("SOFTWARE" in s["nome_servico"].upper() for s in servs)


def test_catmat_price_benchmark(catmat_connector: ComprasGovCatmatConnector) -> None:
    """Validate price benchmark and overpricing risk calculation."""
    bench = catmat_connector.get_price_benchmark("00313")
    assert bench is not None
    assert bench["tipo_item"] == "MATERIAL"
    assert bench["preco_medio_praticado"] > 0
    assert bench["preco_minimo"] <= bench["preco_maximo"]
    assert bench["rating_risco_sobrepreco"] in {"BAIXO", "MEDIO", "ALTO"}

    # Fallback search by description
    bench_desc = catmat_connector.get_price_benchmark("MICROCOMPUTADOR")
    assert bench_desc is not None


def test_catmat_supplier_procurement_history(catmat_connector: ComprasGovCatmatConnector) -> None:
    """Validate supplier government sales query by sanitized CNPJ."""
    sup = catmat_connector.get_supplier_procurement_history("13.099.169/0001-92")
    assert sup is not None
    assert sup["cnpj_fornecedor"] == "13099169000192"
    assert sup["valor_total_fornecido"] > 0

    missing = catmat_connector.get_supplier_procurement_history("00000000000000")
    assert missing is None


def test_catmat_multi_interface_parity(
    temp_gateway_settings: GatewaySettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Validate end-to-end multi-interface parity across SDK, REST API, and MCP."""
    monkeypatch.setenv("BDG_OFFLINE_MODE", "1")

    # 1. SDK Facade
    sdk = BrazilData(settings=temp_gateway_settings)
    sdk.search_catmat("CADEIRA")
    bench = sdk.get_procurement_benchmark("00313")
    assert bench is not None
    assert bench["preco_medio_praticado"] > 0

    # 2. REST API
    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    res_mat = client.get("/api/v1/procurement/materials/search?q=CADEIRA")
    assert res_mat.status_code == 200
    assert len(res_mat.json()) > 0

    res_serv = client.get("/api/v1/procurement/services/search?q=SOFTWARE")
    assert res_serv.status_code == 200
    assert len(res_serv.json()) > 0

    res_bench = client.get("/api/v1/procurement/benchmark/00313")
    assert res_bench.status_code == 200
    assert res_bench.json()["preco_medio_praticado"] > 0

    res_sup = client.get("/api/v1/procurement/supplier/13099169000192")
    assert res_sup.status_code == 200
    assert res_sup.json()["cnpj_fornecedor"] == "13099169000192"

    res_quick = client.get("/api/v1/catmat/CADEIRA")
    assert res_quick.status_code == 200

    # 3. MCP JSON-RPC Server
    dispatcher = MCPDispatcher(settings=temp_gateway_settings)

    # Search materials via MCP
    mcp_search = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bdg_search_catmat_materials",
            "arguments": {"query": "CADEIRA", "limit": 5},
        },
    })
    assert "result" in mcp_search
    assert not mcp_search["result"]["isError"]
    assert "untrusted_gov_data" in mcp_search["result"]["content"][0]["text"]

    # Price benchmark via MCP
    mcp_bench = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_procurement_price_benchmark",
            "arguments": {"termo": "00313"},
        },
    })
    assert "result" in mcp_bench
    assert not mcp_bench["result"]["isError"]
    assert "untrusted_gov_data" in mcp_bench["result"]["content"][0]["text"]
