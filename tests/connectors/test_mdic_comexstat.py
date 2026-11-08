"""Deterministic offline tests for the MDIC Comex Stat connector and interfaces."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
    MDICComexStatConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_mdic_connector_metadata() -> None:
    """Validate connector identifier and contract attributes."""
    conn = MDICComexStatConnector()
    assert conn.source_id == "mdic_comexstat"
    assert conn.test_connection() is True


def test_mdic_extract_and_parquet_generation(tmp_settings: GatewaySettings) -> None:
    """Validate Medallion layers and Parquet generation for all 6 tables."""
    conn = MDICComexStatConnector(settings=tmp_settings)
    res = conn.extract_all()

    assert res["status"] == "SUCCESS"
    assert res["ncm_count"] >= 5
    assert res["paises_count"] >= 5
    assert res["exp_ncm_count"] >= 3
    assert res["imp_ncm_count"] >= 2
    assert res["exp_mun_count"] >= 3
    assert res["imp_mun_count"] >= 2

    silver_dir = tmp_settings.data_dir / "mdic_comexstat" / "silver"
    for name in [
        "comex_ncm_tabela.parquet",
        "comex_paises.parquet",
        "comex_ncm_exportacao.parquet",
        "comex_ncm_importacao.parquet",
        "comex_municipios_exportacao.parquet",
        "comex_municipios_importacao.parquet",
    ]:
        fpath = silver_dir / name
        assert fpath.exists(), f"File {name} should exist in silver layer"
        tab = pq.read_table(fpath)
        assert tab.num_rows > 0


def test_mdic_gold_feature_store_metrics(tmp_settings: GatewaySettings) -> None:
    """Validate Gold feature store calculation and continuous scores."""
    conn = MDICComexStatConnector(settings=tmp_settings)
    conn.extract_all()

    gold_path = (
        tmp_settings.data_dir
        / "mdic_comexstat"
        / "gold"
        / "features_comercio_exterior_brasil.parquet"
    )
    assert gold_path.exists()

    tab = pq.read_table(gold_path)
    records = tab.to_pylist()
    assert len(records) >= 3

    santos = next(r for r in records if r["id_municipio_7"] == "3548500")
    assert santos["total_exportacao_fob_usd"] == 1_150_000_000.0
    assert santos["saldo_balanca_comercial_usd"] == 1_150_000_000.0
    assert santos["score_competitividade_exportadora"] == 100.0
    assert santos["classificacao_balanca"] == "SUPERAVIT_ESTRUTURAL_ALTO"


def test_mdic_get_municipality_and_ncm(tmp_settings: GatewaySettings) -> None:
    """Test querying municipality and NCM commodity trade flows."""
    conn = MDICComexStatConnector(settings=tmp_settings)
    conn.extract_all()

    # Municipality Santos
    mun = conn.get_municipality_trade("3548500")
    assert mun["nome_municipio"] == "Santos"
    assert mun["sigla_uf"] == "SP"
    assert mun["score_competitividade_exportadora"] == 100.0

    # NCM Soja 12019000
    soja = conn.get_ncm_trade("12019000")
    assert "Soja" in soja["descricao_ncm"]
    assert soja["total_exportado_usd"] == 3_750_000_000.0

    # Trade Balance Profile
    profile = conn.get_trade_balance_profile()
    assert profile["status_balanca"] == "SUPERAVIT"
    assert profile["total_exportacao_fob_usd"] > profile["total_importacao_fob_usd"]


def test_mdic_sdk_integration() -> None:
    """Test Python SDK integration through PublicData facade."""
    sdk = BrazilData()
    conn = MDICComexStatConnector(settings=sdk.settings)
    conn.extract_all()

    # 1. Namespace
    mun = sdk.trade.municipality_trade("3548500")
    assert mun["nome_municipio"] == "Santos"

    ncm = sdk.trade.ncm_trade("12019000")
    assert "Soja" in ncm["descricao_ncm"]

    bal = sdk.trade.trade_balance_profile()
    assert bal["status_balanca"] == "SUPERAVIT"

    # 2. Shortcuts
    assert sdk.get_municipality_trade("3548500")["nome_municipio"] == "Santos"
    assert "Soja" in sdk.get_ncm_trade("12019000")["descricao_ncm"]
    assert sdk.get_trade_balance()["status_balanca"] == "SUPERAVIT"


def test_mdic_cli_integration(capsys: any) -> None:
    """Test CLI commands for trade municipality, ncm, balance and comex shortcut."""
    # 1. pdg trade municipality 3548500 --json
    code = cli_main(["trade", "municipality", "3548500", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["nome_municipio"] == "Santos"

    # 2. pdg trade ncm 12019000 --json
    code = cli_main(["trade", "ncm", "12019000", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    ncm_data = json.loads(captured.out)
    assert "Soja" in ncm_data["descricao_ncm"]

    # 3. pdg trade balance --json
    code = cli_main(["trade", "balance", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    bal_data = json.loads(captured.out)
    assert bal_data["status_balanca"] == "SUPERAVIT"

    # 4. pdg comex shortcut
    code = cli_main(["comex", "12019000", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    assert "Soja" in captured.out


def test_mdic_fastapi_endpoints() -> None:
    """Test FastAPI REST endpoints for MDIC Comex Stat."""
    app = create_app()
    client = TestClient(app)

    # Balance
    r1 = client.get("/api/v1/trade/balance")
    assert r1.status_code == 200
    assert r1.json()["status_balanca"] == "SUPERAVIT"

    # Municipality
    r2 = client.get("/api/v1/trade/municipality/3548500")
    assert r2.status_code == 200
    assert r2.json()["nome_municipio"] == "Santos"

    # NCM
    r3 = client.get("/api/v1/trade/ncm/12019000")
    assert r3.status_code == 200
    assert "Soja" in r3.json()["descricao_ncm"]


def test_mdic_mcp_dispatcher() -> None:
    """Test MCP protocol dispatcher for MDIC tools."""
    disp = MCPDispatcher()

    # Tools List
    list_res = disp.handle_request({"jsonrpc": "2.0", "id": 10, "method": "tools/list"})
    tool_names = [t["name"] for t in list_res["result"]["tools"]]
    assert "bdg_get_municipality_trade" in tool_names
    assert "bdg_get_ncm_trade_profile" in tool_names

    # Call bdg_get_municipality_trade
    call_res = disp.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_municipality_trade",
                "arguments": {"id_municipio": "3548500"},
            },
        }
    )
    assert not call_res.get("result", {}).get("isError")
    content = call_res["result"]["content"][0]["text"]
    assert "Santos" in content

    # Call bdg_get_ncm_trade_profile
    call_res2 = disp.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_ncm_trade_profile",
                "arguments": {"codigo_ncm": "12019000"},
            },
        }
    )
    assert not call_res2.get("result", {}).get("isError")
    content2 = call_res2["result"]["content"][0]["text"]
    assert "Soja" in content2
