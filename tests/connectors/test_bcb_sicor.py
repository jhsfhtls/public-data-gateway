"""Deterministic offline tests for BACEN SICOR connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.bcb_sicor import BcbSicorConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_bcb_sicor_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = BcbSicorConnector(settings=tmp_settings)
    assert conn.source_id == "bcb_sicor"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-BCB-SICOR-001"
    assert "cod_operacao_sicor" in conn.contract.primary_keys
    registered = get_connector("bcb_sicor", settings=tmp_settings)
    assert isinstance(registered, BcbSicorConnector)


def test_bcb_sicor_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold BACEN SICOR rural credit feature store."""
    conn = BcbSicorConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "cod_operacao_sicor",
        "id_municipio",
        "nome_produto",
        "valor_custeio_brl",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "bcb_sicor" / "silver"
    expected_rows = {
        "sicor_custeio_municipio_produto.parquet": 24,
        "sicor_investimento_municipio_produto.parquet": 24,
        "sicor_pronaf_proagro_cobertura.parquet": 12,
        "sicor_resumo_municipal.parquet": 12,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "bcb_sicor"
        / "gold"
        / "features_credito_rural_sicor.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["valor_total_sicor_brl"] > 0
        assert row["classificacao_credito_rural_sicor"] in {
            "POLO_AGRONEGOCIO_CREDITO_FORTE",
            "EXPANSAO_RURAL_MODERADA",
            "CREDITO_RURAL_REDUZIDO",
        }


def test_bcb_sicor_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate custeio, investimento, Pronaf/Proagro, municipal summary, and ranking."""
    conn = BcbSicorConnector(settings=tmp_settings)

    goiania_feats = conn.get_sicor_features("5208707")
    assert goiania_feats is not None
    assert goiania_feats["nome_municipio"] == "Goiânia"
    assert goiania_feats["classificacao_credito_rural_sicor"] == "POLO_AGRONEGOCIO_CREDITO_FORTE"

    campinas_pp = conn.get_pronaf_proagro("Campinas")
    assert campinas_pp is not None
    assert campinas_pp["id_municipio"] == "3509502"

    assert len(conn.get_custeio("5208707")) == 2
    assert len(conn.get_investimento("RenovAgro")) == 12
    assert conn.get_municipal_summary("4106902")["qtd_contratos_sicor"] == 3950

    pronaf_top = conn.list_top_municipalities(limit=3, sort_by_pronaf=True)
    assert len(pronaf_top) == 3
    assert pronaf_top[0]["id_municipio"] == "1302603"

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_bcb_sicor_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_sicor_custeio("5208707")) == 2
    assert len(sdk.get_sicor_investimento("4106902")) == 2
    assert sdk.get_sicor_pronaf_proagro("3509502")["id_municipio"] == "3509502"
    assert sdk.get_sicor_municipal_summary("4106902")["qtd_contratos_sicor"] == 3950
    assert sdk.get_sicor_features("5208707")["sigla_uf"] == "GO"
    assert len(sdk.list_top_sicor_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/environment/sicor/custeio/5208707").status_code == 200
    assert client.get("/api/v1/environment/sicor/investimento/4106902").status_code == 200
    assert client.get("/api/v1/environment/sicor/pronaf-proagro/3509502").status_code == 200
    assert client.get("/api/v1/environment/sicor/municipio/4106902").status_code == 200
    res_feat = client.get("/api/v1/environment/sicor/features/5208707")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Goiânia"
    assert client.get("/api/v1/environment/sicor/ranking?limit=5&sort_by_pronaf=true").status_code == 200

    # 3. CLI
    assert cli_main(["sicor", "features", "5208707", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "5208707"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_bcb_sicor_custeio", {"termo": "5208707"}),
        ("bdg_get_bcb_sicor_pronaf_proagro", {"id_municipio_or_name": "3509502"}),
        ("bdg_get_bcb_sicor_credit_features", {"id_municipio_or_name": "5208707"}),
        ("bdg_list_top_bcb_sicor_municipalities", {"limit": 3, "sort_by_pronaf": True}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 918,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
