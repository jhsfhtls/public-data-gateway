"""Tests for catalog metadata, solution packs, and backward-compatibility shims."""

from brazil_data_gateway import BrazilData
from public_data_gateway import PublicData
from public_data_gateway.core.catalog import CatalogRegistry
from public_data_gateway.core.models import SourceEntry


def test_list_sources_pack_and_legacy_product_alias():
    """Verify pack_id and legacy product_id return identical curated catalog slices."""
    registry = CatalogRegistry()
    by_pack = registry.list_sources(pack_id="P01")
    by_product = registry.list_sources(product_id="P01")

    assert len(by_pack) >= 49
    assert [s.source_id for s in by_pack] == [s.source_id for s in by_product]


def test_sdk_facades_preserve_pack_and_product_filters():
    """Verify PublicData and BrazilData shims behave identically for pack/product queries."""
    pdg = PublicData()
    bdg = BrazilData()

    pdg_sources = pdg.list_sources(pack_id="P02")
    bdg_sources = bdg.list_sources(product_id="P02")

    assert len(pdg_sources) > 0
    assert [s.source_id for s in pdg_sources] == [s.source_id for s in bdg_sources]


def test_source_entry_accepts_solution_and_legacy_product_packs():
    """Verify SourceEntry accepts both solution_packs and product_packs field names."""
    base_kwargs = {
        "source_id": "test_source",
        "name": "Test Source",
        "agency": "TEST",
        "official_url": "https://example.gov.br",
        "segment_id": "01_teste",
        "segment_name": "Teste",
        "access_mode": "rest_api",
        "protocol": "https_json",
    }

    entry_new = SourceEntry(**base_kwargs, solution_packs=["P01", "P05"])
    entry_legacy = SourceEntry(**base_kwargs, product_packs=["P01", "P05"])

    assert entry_new.solution_packs == ["P01", "P05"]
    assert entry_legacy.solution_packs == ["P01", "P05"]


def test_unified_single_catalog_114_sources_and_search():
    """Verify unified single catalog contains all 114 sources (85 GOV_BR + 29 PUBLIC_GLOBAL) and supports keyword search."""
    from pathlib import Path

    from fastapi.testclient import TestClient

    from public_data_gateway.api.app import create_app
    from public_data_gateway.cli.main import main as cli_main
    from public_data_gateway.core.connector import get_connector, list_registered_connectors
    from public_data_gateway.core.contracts import load_contract
    from public_data_gateway.mcp.server import MCPDispatcher

    registry = CatalogRegistry()
    all_sources = registry.list_sources()
    gov_sources = registry.list_sources(source_origin="GOV_BR")
    pub_sources = registry.list_sources(source_origin="PUBLIC_GLOBAL")

    assert len(all_sources) == 114
    assert len(gov_sources) == 85
    assert len(pub_sources) == 29
    assert not (Path("catalog") / "public_apis_registry.yaml").exists()

    # Keyword & accent-insensitive search for agents
    bf_hits = registry.list_sources(query="bolsa familia")
    assert any(s.source_id == "cgu_beneficios_cidadao" for s in bf_hits)
    osm_hits = registry.list_sources(query="overpass")
    assert any(s.source_id == "overpass_osm" for s in osm_hits)

    # All 114 connectors registered in _CONNECTOR_REGISTRY and loadable via load_contract
    reg_ids = list_registered_connectors()
    assert len(reg_ids) == 114
    for legacy_id in (
        "comprasgov_catmat",
        "ibge_censo2022",
        "ibge_cnefe",
        "ibge_pnad_continua",
        "ibge_sidra",
        "mte_novo_caged",
        "mte_rais",
    ):
        assert legacy_id in reg_ids
        assert get_connector(legacy_id) is not None

    for contract_file in Path("src/public_data_gateway/connectors").glob("*/*/contract.yaml"):
        contract = load_contract(contract_file)
        assert contract.contract_id
        assert contract.source_id

    # 4-interface unified catalog search parity
    pdg = PublicData()
    assert len(pdg.list_sources(source_origin="PUBLIC_GLOBAL")) == 29
    assert any(s.source_id == "rfb_arrecadacao" for s in pdg.list_sources(query="pgfn"))

    client = TestClient(create_app())
    r_pub = client.get("/api/v1/catalog?origin=PUBLIC_GLOBAL")
    assert r_pub.status_code == 200 and len(r_pub.json()) == 29
    r_q = client.get("/api/v1/catalog?q=munic")
    assert r_q.status_code == 200 and any(x["source_id"] == "ibge_institucional" for x in r_q.json())

    assert cli_main(["catalog", "--origin", "PUBLIC_GLOBAL", "--query", "openalex", "--json"]) == 0

    mcp = MCPDispatcher()
    mcp_res = mcp.handle_request({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "bdg_list_sources", "arguments": {"origin": "PUBLIC_GLOBAL", "query": "b3"}},
    })
    assert mcp_res["result"]["isError"] is False
    assert "brapi_b3" in mcp_res["result"]["content"][0]["text"]
