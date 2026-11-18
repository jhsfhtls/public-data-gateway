"""Deterministic offline tests for RFB CAFIR/CNIR connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.rfb_cafir import RfbCafirConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_rfb_cafir_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = RfbCafirConnector(settings=tmp_settings)
    assert conn.source_id == "rfb_cafir"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-RFB-CAFIR-001"
    assert "nirf_cib" in conn.contract.primary_keys
    registered = get_connector("rfb_cafir", settings=tmp_settings)
    assert isinstance(registered, RfbCafirConnector)


def test_rfb_cafir_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold RFB CAFIR land regularization feature store."""
    conn = RfbCafirConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "nirf_cib",
        "codigo_incra_sncr",
        "documento_titular",
        "area_total_ha",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "rfb_cafir" / "silver"
    expected_rows = {
        "cafir_imoveis_rurais.parquet": 24,
        "cafir_vinculos_cnir_sncr.parquet": 12,
        "cafir_concentracao_fundiaria.parquet": 12,
        "cafir_resumo_municipal.parquet": 12,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "rfb_cafir"
        / "gold"
        / "features_fundiario_cafir.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["area_total_cafir_ha"] > 0
        assert row["classificacao_estrutura_fundiaria_cafir"] in {
            "ESTRUTURA_CNIR_REGULARIZADA",
            "CONSOLIDACAO_CADASTRAL_PARCIAL",
            "BAIXA_VINCULACAO_CNIR_CAFIR",
        }


def test_rfb_cafir_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate rural properties, CNIR linkage, concentration Gini, summary, and ranking."""
    conn = RfbCafirConnector(settings=tmp_settings)

    curitiba_feats = conn.get_cafir_features("4106902")
    assert curitiba_feats is not None
    assert curitiba_feats["nome_municipio"] == "Curitiba"
    assert curitiba_feats["classificacao_estrutura_fundiaria_cafir"] == "ESTRUTURA_CNIR_REGULARIZADA"

    campinas_cnir = conn.get_cnir_linkage("Campinas")
    assert campinas_cnir is not None
    assert campinas_cnir["id_municipio"] == "3509502"

    assert len(conn.get_rural_properties("5208707")) == 2
    assert conn.get_land_concentration("5208707")["indice_gini_fundiario_cafir"] == 0.67
    assert conn.get_municipal_summary("4106902")["qtd_imoveis_cafir"] == 3420

    area_top = conn.list_top_municipalities(limit=3, sort_by_area=True)
    assert len(area_top) == 3
    assert area_top[0]["id_municipio"] == "5208707"

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_rfb_cafir_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_cafir_rural_properties("4106902")) == 2
    assert sdk.get_cafir_cnir_linkage("3509502")["id_municipio"] == "3509502"
    assert sdk.get_cafir_land_concentration("5208707")["indice_gini_fundiario_cafir"] == 0.67
    assert sdk.get_cafir_municipal_summary("4106902")["qtd_imoveis_cafir"] == 3420
    assert sdk.get_cafir_features("4106902")["sigla_uf"] == "PR"
    assert len(sdk.list_top_cafir_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/environment/cafir/imoveis/4106902").status_code == 200
    assert client.get("/api/v1/environment/cafir/cnir/3509502").status_code == 200
    assert client.get("/api/v1/environment/cafir/concentracao/5208707").status_code == 200
    assert client.get("/api/v1/environment/cafir/municipio/4106902").status_code == 200
    res_feat = client.get("/api/v1/environment/cafir/features/4106902")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Curitiba"
    assert client.get("/api/v1/environment/cafir/ranking?limit=5&sort_by_area=true").status_code == 200

    # 3. CLI
    assert cli_main(["cafir", "features", "4106902", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "4106902"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_rfb_cafir_properties", {"termo": "4106902"}),
        ("bdg_get_rfb_cafir_cnir_linkage", {"id_municipio_or_name": "3509502"}),
        ("bdg_get_rfb_cafir_land_features", {"id_municipio_or_name": "4106902"}),
        ("bdg_list_top_rfb_cafir_municipalities", {"limit": 3, "sort_by_area": True}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 920,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
