"""Deterministic offline tests for STN CAPAG & FIRJAN IFGF connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.stn_capag import StnCapagConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_stn_capag_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = StnCapagConnector(settings=tmp_settings)
    assert conn.source_id == "stn_capag"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-STN-CAPAG-001"
    assert "id_municipio" in conn.contract.primary_keys
    registered = get_connector("stn_capag", settings=tmp_settings)
    assert isinstance(registered, StnCapagConnector)


def test_stn_capag_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold CAPAG sovereign rating feature store."""
    conn = StnCapagConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 12
    assert {
        "id_municipio",
        "sigla_uf",
        "nota_capag_oficial",
        "elegivel_garantia_uniao",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "stn_capag" / "silver"
    for fname in (
        "capag_avaliacao_municipal.parquet",
        "capag_avaliacao_estadual.parquet",
        "capag_ifgf_gestao_fiscal.parquet",
        "capag_garantias_operacoes.parquet",
    ):
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == 12

    gold_path = (
        tmp_settings.data_dir
        / "stn_capag"
        / "gold"
        / "features_capacidade_pagamento_capag.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert 0.0 <= row["ifgf_geral"] <= 1.0
        assert row["classificacao_rating_subnacional"] in {
            "GRAU_INVESTIMENTO_GARANTIA_UNIAO",
            "CAPACIDADE_MODERADA_OBSERVACAO",
            "ALTO_RISCO_RESTRICAO_GARANTIA",
        }


def test_stn_capag_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate municipal/state CAPAG, IFGF, sovereign guarantees, features, and ranking."""
    conn = StnCapagConnector(settings=tmp_settings)

    manaus_feats = conn.get_capag_features("1302603")
    assert manaus_feats is not None
    assert manaus_feats["nome_municipio"] == "Manaus"
    assert manaus_feats["nota_capag_oficial"] == "A"
    assert manaus_feats["elegivel_garantia_uniao"] is True

    campinas_ifgf = conn.get_ifgf_index("Campinas")
    assert campinas_ifgf is not None
    assert campinas_ifgf["id_municipio"] == "3509502"
    assert campinas_ifgf["conceito_ifgf"] == "GESTAO_EXCELENCIA"

    assert len(conn.get_state_evaluation("SP")) == 2
    assert len(conn.get_sovereign_guarantees("1302603")) == 1

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_stn_capag_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert sdk.get_capag_municipal_evaluation("1302603")["nota_capag_oficial"] == "A"
    assert len(sdk.get_capag_state_evaluation("SP")) == 2
    assert sdk.get_capag_ifgf_index("3509502")["id_municipio"] == "3509502"
    assert len(sdk.get_capag_sovereign_guarantees("1302603")) == 1
    assert sdk.get_capag_features("3509502")["sigla_uf"] == "SP"
    assert len(sdk.list_top_capag_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/treasury/capag/municipio/1302603").status_code == 200
    assert client.get("/api/v1/treasury/capag/estado/SP").status_code == 200
    assert client.get("/api/v1/treasury/capag/ifgf/3509502").status_code == 200
    assert client.get("/api/v1/treasury/capag/garantias/1302603").status_code == 200
    res_feat = client.get("/api/v1/treasury/capag/features/1302603")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Manaus"
    assert client.get("/api/v1/treasury/capag/ranking?limit=5&uf=SP").status_code == 200

    # 3. CLI
    assert cli_main(["capag", "features", "1302603", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1302603"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_capag_municipal_evaluation", {"id_municipio_or_name": "1302603"}),
        ("bdg_get_capag_ifgf_gestao_fiscal", {"id_municipio_or_name": "3509502"}),
        ("bdg_get_capag_sovereign_features", {"id_municipio_or_name": "1302603"}),
        ("bdg_list_top_capag_municipalities", {"limit": 3, "sigla_uf": "SP"}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 913,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
