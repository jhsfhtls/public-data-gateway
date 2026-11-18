"""Deterministic offline tests for IBGE Agro Municipal connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
    IbgeAgroMunicipalConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_ibge_agro_municipal_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = IbgeAgroMunicipalConnector(settings=tmp_settings)
    assert conn.source_id == "ibge_agro_municipal"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-IBGE-AGRO-001"
    assert "cod_registro_pam" in conn.contract.primary_keys
    registered = get_connector("ibge_agro_municipal", settings=tmp_settings)
    assert isinstance(registered, IbgeAgroMunicipalConnector)


def test_ibge_agro_municipal_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold IBGE Agro Municipal feature store."""
    conn = IbgeAgroMunicipalConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "cod_registro_pam",
        "id_municipio",
        "nome_cultura",
        "valor_producao_mil_brl",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "ibge_agro_municipal" / "silver"
    expected_rows = {
        "ibge_pam_lavouras.parquet": 24,
        "ibge_ppm_pecuaria.parquet": 24,
        "ibge_pevs_silvicultura.parquet": 12,
        "ibge_agro_resumo_municipal.parquet": 12,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "ibge_agro_municipal"
        / "gold"
        / "features_producao_agro_ibge.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["valor_bruto_agro_total_mil_brl"] > 0
        assert row["classificacao_vocacao_agro_ibge"] in {
            "POLO_AGROPECUARIO_ALTA_DENSIDADE",
            "PRODUCAO_AGRO_DIVERSIFICADA",
            "BAIXA_VOCACAO_AGROPECUARIA",
        }


def test_ibge_agro_municipal_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate PAM, PPM, PEVS, municipal summary, and ranking."""
    conn = IbgeAgroMunicipalConnector(settings=tmp_settings)

    goiania_feats = conn.get_agro_features("5208707")
    assert goiania_feats is not None
    assert goiania_feats["nome_municipio"] == "Goiânia"
    assert goiania_feats["classificacao_vocacao_agro_ibge"] == "POLO_AGROPECUARIO_ALTA_DENSIDADE"

    campinas_pevs = conn.get_pevs_forestry("Campinas")
    assert campinas_pevs is not None
    assert campinas_pevs["id_municipio"] == "3509502"

    assert len(conn.get_pam_crops("5208707")) == 2
    assert len(conn.get_ppm_livestock("Bovino")) == 12
    assert conn.get_municipal_summary("4106902")["efetivo_rebanho_bovino_cabecas"] == 240000

    vbp_top = conn.list_top_municipalities(limit=3, sort_by_vbp=True)
    assert len(vbp_top) == 3
    assert vbp_top[0]["id_municipio"] == "5208707"

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_ibge_agro_municipal_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_agro_pam_crops("5208707")) == 2
    assert len(sdk.get_agro_ppm_livestock("4106902")) == 2
    assert sdk.get_agro_pevs_forestry("3509502")["id_municipio"] == "3509502"
    assert sdk.get_agro_municipal_summary("4106902")["efetivo_rebanho_bovino_cabecas"] == 240000
    assert sdk.get_agro_features("5208707")["sigla_uf"] == "GO"
    assert len(sdk.list_top_agro_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/environment/agro/pam/5208707").status_code == 200
    assert client.get("/api/v1/environment/agro/ppm/4106902").status_code == 200
    assert client.get("/api/v1/environment/agro/pevs/3509502").status_code == 200
    assert client.get("/api/v1/environment/agro/municipio/4106902").status_code == 200
    res_feat = client.get("/api/v1/environment/agro/features/5208707")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Goiânia"
    assert client.get("/api/v1/environment/agro/ranking?limit=5&sort_by_vbp=true").status_code == 200

    # 3. CLI
    assert cli_main(["agro", "features", "5208707", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "5208707"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_ibge_agro_pam_crops", {"termo": "5208707"}),
        ("bdg_get_ibge_agro_ppm_livestock", {"termo": "4106902"}),
        ("bdg_get_ibge_agro_production_features", {"id_municipio_or_name": "5208707"}),
        ("bdg_list_top_ibge_agro_municipalities", {"limit": 3, "sort_by_vbp": True}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 919,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
