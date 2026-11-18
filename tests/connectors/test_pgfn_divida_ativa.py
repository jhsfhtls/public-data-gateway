"""Deterministic offline tests for PGFN Dívida Ativa connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.pgfn_divida_ativa import PgfnDividaAtivaConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_pgfn_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = PgfnDividaAtivaConnector(settings=tmp_settings)
    assert conn.source_id == "pgfn_divida_ativa"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-PGFN-DIVIDA-001"
    assert "numero_inscricao" in conn.contract.primary_keys
    registered = get_connector("pgfn_divida_ativa", settings=tmp_settings)
    assert isinstance(registered, PgfnDividaAtivaConnector)


def test_pgfn_medallion_silver_and_gold_parquets(tmp_settings: GatewaySettings) -> None:
    """Validate all 4 Silver Parquets and Gold PGFN tax risk feature store generation."""
    conn = PgfnDividaAtivaConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {"numero_inscricao", "documento_devedor", "id_municipio", "sigla_uf"}.issubset(
        tab.column_names
    )

    silver_dir = tmp_settings.data_dir / "pgfn_divida_ativa" / "silver"
    for fname in (
        "pgfn_inscricoes_gerais.parquet",
        "pgfn_divida_previdenciaria.parquet",
        "pgfn_divida_fgts.parquet",
        "pgfn_resumo_municipal.parquet",
    ):
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows >= 12

    gold_path = (
        tmp_settings.data_dir / "pgfn_divida_ativa" / "gold" / "features_risco_divida_ativa_pgfn.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["valor_total_consolidado_brl"] > 0
        assert row["classificacao_risco_pgfn"] in {
            "ALTO_RISCO_EXPOSICAO_PGFN",
            "RISCO_MODERADO_PGFN",
            "BAIXO_RISCO_PGFN",
        }


def test_pgfn_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate debtor, social security, FGTS, municipal summary, and ranking queries."""
    conn = PgfnDividaAtivaConnector(settings=tmp_settings)

    manaus_feats = conn.get_pgfn_features("1302603")
    assert manaus_feats is not None
    assert manaus_feats["nome_municipio"] == "Manaus"
    assert manaus_feats["valor_total_consolidado_brl"] > 0

    campinas_summary = conn.get_municipal_summary("Campinas")
    assert campinas_summary is not None
    assert campinas_summary["id_municipio"] == "3509502"

    assert len(conn.get_debtor_inscriptions("1302603")) == 2
    assert len(conn.get_previdenciario_debts("1302603")) == 1
    assert len(conn.get_fgts_debts("1302603")) == 1

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_pgfn_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_pgfn_debtor("1302603")) == 2
    assert len(sdk.get_pgfn_previdenciario("1302603")) == 1
    assert len(sdk.get_pgfn_fgts("1302603")) == 1
    assert sdk.get_pgfn_municipal_summary("1302603")["sigla_uf"] == "AM"
    assert sdk.get_pgfn_features("3509502")["nome_municipio"] == "Campinas"
    assert len(sdk.list_top_pgfn_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/compliance/pgfn/debtor/1302603").status_code == 200
    assert client.get("/api/v1/compliance/pgfn/previdenciario/1302603").status_code == 200
    assert client.get("/api/v1/compliance/pgfn/fgts/1302603").status_code == 200
    assert client.get("/api/v1/compliance/pgfn/municipio/1302603").status_code == 200
    res_feat = client.get("/api/v1/compliance/pgfn/features/1302603")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Manaus"
    assert client.get("/api/v1/compliance/pgfn/ranking?limit=5&uf=SP").status_code == 200

    # 3. CLI
    assert cli_main(["pgfn", "features", "1302603", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1302603"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_pgfn_debtor_inscriptions", {"documento_or_name": "1302603"}),
        ("bdg_get_pgfn_municipal_summary", {"id_municipio_or_name": "1302603"}),
        ("bdg_get_pgfn_risk_features", {"id_municipio_or_name": "3509502"}),
        ("bdg_list_top_pgfn_municipalities", {"limit": 3, "sigla_uf": "SP"}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 911,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
