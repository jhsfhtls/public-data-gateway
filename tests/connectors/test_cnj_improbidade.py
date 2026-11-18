"""Deterministic offline tests for CNJ CNCIAI Improbidade connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.judicial.cnj_improbidade import (
    CnjImprobidadeConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_cnj_improbidade_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = CnjImprobidadeConnector(settings=tmp_settings)
    assert conn.source_id == "cnj_improbidade"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-CNJ-IMPROBIDADE-001"
    assert "id_condenacao" in conn.contract.primary_keys
    registered = get_connector("cnj_improbidade", settings=tmp_settings)
    assert isinstance(registered, CnjImprobidadeConnector)


def test_cnj_improbidade_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold CNJ CNCIAI misconduct risk feature store."""
    conn = CnjImprobidadeConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "id_condenacao",
        "numero_processo_cnj",
        "documento_requerido",
        "id_municipio",
        "sigla_uf",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "cnj_improbidade" / "silver"
    expected_silver = {
        "cnciai_condenacoes.parquet": 24,
        "cnciai_sancoes_contratar.parquet": 12,
        "cnciai_ressarcimento_erario.parquet": 12,
        "cnciai_resumo_municipal.parquet": 12,
    }
    for fname, min_rows in expected_silver.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == min_rows

    gold_path = (
        tmp_settings.data_dir
        / "cnj_improbidade"
        / "gold"
        / "features_risco_improbidade_cnciai.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["valor_total_ressarcimento_multa_brl"] > 0
        assert row["classificacao_risco_improbidade"] in {
            "ALTA_EXPOSICAO_CNCIAI",
            "MODERADA_EXPOSICAO_CNCIAI",
            "BAIXA_EXPOSICAO_CNCIAI",
        }


def test_cnj_improbidade_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate condemnations, debarments, reimbursements, summary, features, and ranking."""
    conn = CnjImprobidadeConnector(settings=tmp_settings)

    manaus_feats = conn.get_improbidade_features("1302603")
    assert manaus_feats is not None
    assert manaus_feats["nome_municipio"] == "Manaus"
    assert manaus_feats["valor_total_ressarcimento_multa_brl"] > 0
    assert manaus_feats["classificacao_risco_improbidade"] == "MODERADA_EXPOSICAO_CNCIAI"

    campinas_summary = conn.get_municipal_summary("Campinas")
    assert campinas_summary is not None
    assert campinas_summary["id_municipio"] == "3509502"

    assert len(conn.get_condemnations("1302603")) == 2
    assert len(conn.get_debarment_sanctions("1302603")) == 1
    assert len(conn.get_treasury_reimbursements("1302603")) == 1

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_cnj_improbidade_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_improbidade_condenacoes("1302603")) == 2
    assert len(sdk.get_improbidade_proibicoes_contratar("1302603")) == 1
    assert len(sdk.get_improbidade_ressarcimentos("1302603")) == 1
    assert sdk.get_improbidade_municipal_summary("1302603")["sigla_uf"] == "AM"
    assert sdk.get_improbidade_features("3509502")["nome_municipio"] == "Campinas"
    assert len(sdk.list_top_improbidade_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/judicial/improbidade/condenacoes/1302603").status_code == 200
    assert client.get("/api/v1/judicial/improbidade/sancoes/1302603").status_code == 200
    assert client.get("/api/v1/judicial/improbidade/ressarcimento/1302603").status_code == 200
    assert client.get("/api/v1/judicial/improbidade/municipio/1302603").status_code == 200
    res_feat = client.get("/api/v1/judicial/improbidade/features/1302603")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Manaus"
    assert client.get("/api/v1/judicial/improbidade/ranking?limit=5&uf=SP").status_code == 200

    # 3. CLI
    assert cli_main(["improbidade", "features", "1302603", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1302603"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_cnciai_improbidade_condemnations", {"documento_or_name": "1302603"}),
        ("bdg_get_cnciai_municipal_summary", {"id_municipio_or_name": "1302603"}),
        ("bdg_get_cnciai_risk_features", {"id_municipio_or_name": "3509502"}),
        ("bdg_list_top_cnciai_municipalities", {"limit": 3, "sigla_uf": "SP"}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 912,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
