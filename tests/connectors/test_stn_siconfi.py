"""Deterministic offline tests for STN SICONFI connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import StnSiconfiConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_stn_siconfi_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = StnSiconfiConnector(settings=tmp_settings)
    assert conn.source_id == "stn_siconfi"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-STN-SICONFI-001"
    assert "id_municipio" in conn.contract.primary_keys
    registered = get_connector("stn_siconfi", settings=tmp_settings)
    assert isinstance(registered, StnSiconfiConnector)


def test_stn_siconfi_medallion_silver_and_gold_parquets(tmp_settings: GatewaySettings) -> None:
    """Validate all 5 Silver Parquets and Gold SICONFI feature store generation."""
    conn = StnSiconfiConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 12
    assert {"id_municipio", "sigla_uf", "cnpj_14"}.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "stn_siconfi" / "silver"
    for fname in (
        "siconfi_entes.parquet",
        "siconfi_dca_balanco.parquet",
        "siconfi_rreo_execucao.parquet",
        "siconfi_rgf_limites.parquet",
        "siconfi_extratos_entregas.parquet",
    ):
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows >= 12

    gold_path = (
        tmp_settings.data_dir / "stn_siconfi" / "gold" / "features_contabilidade_fiscal_siconfi.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert len(row["cnpj_14"]) == 14
        assert row["status_alerta_lrf"] in {
            "REGULAR",
            "ALERTA_48_6",
            "PRUDENCIAL_51_3",
            "ACIMA_LIMITE_54",
        }
        assert 10.0 <= row["score_solidez_contabil_siconfi"] <= 99.0


def test_stn_siconfi_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate lookups for Manaus (1302603), Campinas (3509502), and UF ranking."""
    conn = StnSiconfiConnector(settings=tmp_settings)

    manaus = conn.get_siconfi_features("1302603")
    assert manaus is not None
    assert manaus["nome_ente"] == "Manaus"
    assert manaus["sigla_uf"] == "AM"
    assert manaus["despesa_total_pessoal_brl"] > 0

    campinas = conn.get_ente_profile("Campinas")
    assert campinas is not None
    assert campinas["id_municipio"] == "3509502"

    assert len(conn.get_dca_balance("1302603", ano=2024)) == 1
    assert len(conn.get_rreo_execution("1302603", ano=2024)) == 2
    assert len(conn.get_rgf_limits("1302603", ano=2024)) == 2
    assert len(conn.get_delivery_extracts("1302603", ano=2024)) == 4

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_stn_siconfi_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert sdk.get_siconfi_ente("1302603")["nome_ente"] == "Manaus"
    assert len(sdk.get_siconfi_dca("1302603")) == 1
    assert len(sdk.get_siconfi_rreo("1302603")) == 2
    assert len(sdk.get_siconfi_rgf("1302603")) == 2
    assert len(sdk.get_siconfi_extratos("1302603")) == 4
    assert sdk.get_siconfi_features("3509502")["sigla_uf"] == "SP"
    assert len(sdk.list_top_siconfi_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/treasury/siconfi/ente/1302603").status_code == 200
    assert client.get("/api/v1/treasury/siconfi/dca/1302603").status_code == 200
    assert client.get("/api/v1/treasury/siconfi/rreo/1302603").status_code == 200
    assert client.get("/api/v1/treasury/siconfi/rgf/1302603").status_code == 200
    assert client.get("/api/v1/treasury/siconfi/extratos/1302603").status_code == 200
    res_feat = client.get("/api/v1/treasury/siconfi/features/1302603")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_ente"] == "Manaus"
    assert client.get("/api/v1/treasury/siconfi/ranking?limit=5&uf=SP").status_code == 200

    # 3. CLI
    assert cli_main(["siconfi", "features", "1302603", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1302603"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_siconfi_fiscal_profile", {"id_municipio_or_name": "1302603"}),
        ("bdg_get_siconfi_rgf_limits", {"id_municipio": "1302603", "ano": 2024}),
        ("bdg_get_siconfi_dca_balance", {"id_municipio": "1302603", "ano": 2024}),
        ("bdg_list_top_siconfi_municipalities", {"limit": 3, "sigla_uf": "SP"}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 901,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
