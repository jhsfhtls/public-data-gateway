"""Deterministic offline tests for BACEN ESTBAN connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.bcb_estban import BcbEstbanConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_bcb_estban_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = BcbEstbanConnector(settings=tmp_settings)
    assert conn.source_id == "bcb_estban"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-BCB-ESTBAN-001"
    assert "id_municipio" in conn.contract.primary_keys
    registered = get_connector("bcb_estban", settings=tmp_settings)
    assert isinstance(registered, BcbEstbanConnector)


def test_bcb_estban_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold ESTBAN banking liquidity feature store."""
    conn = BcbEstbanConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 12
    assert {
        "id_municipio",
        "sigla_uf",
        "operacoes_credito_total_brl",
        "depositos_totais_brl",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "bcb_estban" / "silver"
    expected_rows = {
        "estban_balancete_municipal.parquet": 12,
        "estban_carteira_credito_setorial.parquet": 12,
        "estban_captacoes_depositos.parquet": 12,
        "estban_instituicoes_municipio.parquet": 24,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "bcb_estban"
        / "gold"
        / "features_liquidez_credito_estban.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["operacoes_credito_total_brl"] > 0
        assert row["classificacao_mercado_bancario_local"] in {
            "POLO_FINANCEIRO_ALTA_LIQUIDEZ",
            "MERCADO_BANCARIO_CONSOLIDADO",
            "MERCADO_BANCARIO_EM_EXPANSAO",
        }


def test_bcb_estban_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate balance, credit portfolio, deposits, local institutions, features, and ranking."""
    conn = BcbEstbanConnector(settings=tmp_settings)

    manaus_feats = conn.get_estban_features("1302603")
    assert manaus_feats is not None
    assert manaus_feats["nome_municipio"] == "Manaus"
    assert manaus_feats["qtd_agencias_ativas"] == 148
    assert manaus_feats["classificacao_mercado_bancario_local"] == "MERCADO_BANCARIO_CONSOLIDADO"

    campinas_bal = conn.get_municipal_balance("Campinas")
    assert campinas_bal is not None
    assert campinas_bal["id_municipio"] == "3509502"

    assert conn.get_credit_portfolio("1302603")["financiamento_agropecuario_rural_brl"] > 0
    assert conn.get_deposit_funding("1302603")["depositos_poupanca_brl"] > 0
    assert len(conn.get_local_institutions("1302603")) == 2

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_bcb_estban_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert sdk.get_estban_municipal_balance("1302603")["qtd_instituicoes_atuantes"] == 15
    assert sdk.get_estban_credit_portfolio("3509502")["id_municipio"] == "3509502"
    assert sdk.get_estban_deposit_funding("1302603")["depositos_vista_brl"] > 0
    assert len(sdk.get_estban_local_institutions("1302603")) == 2
    assert sdk.get_estban_features("3509502")["sigla_uf"] == "SP"
    assert len(sdk.list_top_estban_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/banking/estban/balancete/1302603").status_code == 200
    assert client.get("/api/v1/banking/estban/credito/3509502").status_code == 200
    assert client.get("/api/v1/banking/estban/depositos/1302603").status_code == 200
    assert client.get("/api/v1/banking/estban/instituicoes/1302603").status_code == 200
    res_feat = client.get("/api/v1/banking/estban/features/1302603")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Manaus"
    assert client.get("/api/v1/banking/estban/ranking?limit=5&uf=SP").status_code == 200

    # 3. CLI
    assert cli_main(["estban", "features", "1302603", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1302603"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_estban_municipal_balance", {"id_municipio_or_name": "1302603"}),
        ("bdg_get_estban_credit_portfolio", {"id_municipio_or_name": "3509502"}),
        ("bdg_get_estban_liquidity_features", {"id_municipio_or_name": "1302603"}),
        ("bdg_list_top_estban_municipalities", {"limit": 3, "sigla_uf": "SP"}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 914,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
