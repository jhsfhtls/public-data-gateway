"""Deterministic offline tests for RFB CNO connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.empresarial.rfb_cno import RfbCnoConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_rfb_cno_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = RfbCnoConnector(settings=tmp_settings)
    assert conn.source_id == "rfb_cno"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-RFB-CNO-001"
    assert "inscricao_cno" in conn.contract.primary_keys
    registered = get_connector("rfb_cno", settings=tmp_settings)
    assert isinstance(registered, RfbCnoConnector)


def test_rfb_cno_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold RFB CNO construction dynamism feature store."""
    conn = RfbCnoConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "inscricao_cno",
        "documento_responsavel",
        "id_municipio",
        "area_total_m2",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "rfb_cno" / "silver"
    expected_rows = {
        "cno_obras_cadastradas.parquet": 24,
        "cno_areas_destinacao.parquet": 12,
        "cno_vinculos_responsaveis.parquet": 24,
        "cno_resumo_municipal.parquet": 12,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "rfb_cno"
        / "gold"
        / "features_dinamismo_obras_cno.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["area_total_registrada_m2"] > 0
        assert row["classificacao_expansao_imobiliaria_cno"] in {
            "POLO_EXPANSAO_CONSTRUTIVA_ALTA",
            "DINAMISMO_CONSTRUTIVO_MODERADO",
            "BAIXA_ATIVIDADE_CNO",
        }


def test_rfb_cno_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate works, built areas, responsible links, municipal summary, and ranking."""
    conn = RfbCnoConnector(settings=tmp_settings)

    manaus_feats = conn.get_cno_features("1302603")
    assert manaus_feats is not None
    assert manaus_feats["nome_municipio"] == "Manaus"
    assert manaus_feats["total_obras_cno"] == 6420
    assert manaus_feats["classificacao_expansao_imobiliaria_cno"] == "DINAMISMO_CONSTRUTIVO_MODERADO"

    campinas_area = conn.get_area_breakdown("Campinas")
    assert campinas_area is not None
    assert campinas_area["id_municipio"] == "3509502"

    assert len(conn.get_works("1302603")) == 2
    assert len(conn.get_responsible_links("1302603")) == 2
    assert conn.get_municipal_summary("1302603")["obras_ativas_em_andamento"] == 4380

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_rfb_cno_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_cno_works("1302603")) == 2
    assert sdk.get_cno_area_breakdown("3509502")["id_municipio"] == "3509502"
    assert len(sdk.get_cno_responsible_links("1302603")) == 2
    assert sdk.get_cno_municipal_summary("1302603")["obras_ativas_em_andamento"] == 4380
    assert sdk.get_cno_features("3509502")["sigla_uf"] == "SP"
    assert len(sdk.list_top_cno_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/company/cno/obras/1302603").status_code == 200
    assert client.get("/api/v1/company/cno/areas/3509502").status_code == 200
    assert client.get("/api/v1/company/cno/vinculos/1302603").status_code == 200
    assert client.get("/api/v1/company/cno/municipio/1302603").status_code == 200
    res_feat = client.get("/api/v1/company/cno/features/1302603")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Manaus"
    assert client.get("/api/v1/company/cno/ranking?limit=5&uf=SP").status_code == 200

    # 3. CLI
    assert cli_main(["cno", "features", "1302603", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1302603"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_rfb_cno_works", {"termo": "1302603"}),
        ("bdg_get_rfb_cno_municipal_summary", {"id_municipio_or_name": "3509502"}),
        ("bdg_get_rfb_cno_construction_features", {"id_municipio_or_name": "1302603"}),
        ("bdg_list_top_rfb_cno_municipalities", {"limit": 3, "sigla_uf": "SP"}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 916,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
