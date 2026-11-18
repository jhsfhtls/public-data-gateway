"""Deterministic offline tests for INPE Queimadas & SEEG connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
    InpeQueimadasSeegConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_inpe_queimadas_seeg_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = InpeQueimadasSeegConnector(settings=tmp_settings)
    assert conn.source_id == "inpe_queimadas_seeg"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-INPE-QUEIMADAS-SEEG-001"
    assert "cod_foco_inpe" in conn.contract.primary_keys
    registered = get_connector("inpe_queimadas_seeg", settings=tmp_settings)
    assert isinstance(registered, InpeQueimadasSeegConnector)


def test_inpe_queimadas_seeg_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold Queimadas/SEEG climate risk feature store."""
    conn = InpeQueimadasSeegConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "cod_foco_inpe",
        "satelite_referencia",
        "risco_fogo",
        "frp_mw",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "inpe_queimadas_seeg" / "silver"
    expected_rows = {
        "inpe_focos_queimadas.parquet": 24,
        "seeg_emissoes_gee_setor.parquet": 24,
        "inpe_risco_fogo_mensal.parquet": 12,
        "queimadas_seeg_resumo_municipal.parquet": 12,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "inpe_queimadas_seeg"
        / "gold"
        / "features_queimadas_emissoes_gee.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["emissoes_liquidas_tco2e_seeg"] > 0
        assert row["classificacao_risco_climatico_queimadas"] in {
            "BAIXO_RISCO_FOGO_GEE",
            "ALERTA_FOGO_EMISSOES_MODERADAS",
            "CRITICO_QUEIMADAS_EMISSOES_GEE",
        }


def test_inpe_queimadas_seeg_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate fire hotspots, SEEG emissions, fire risk, summary, and ranking."""
    conn = InpeQueimadasSeegConnector(settings=tmp_settings)

    belem_feats = conn.get_queimadas_seeg_features("1501402")
    assert belem_feats is not None
    assert belem_feats["nome_municipio"] == "Belém"
    assert belem_feats["classificacao_risco_climatico_queimadas"] == "CRITICO_QUEIMADAS_EMISSOES_GEE"

    campinas_risco = conn.get_fire_risk("Campinas")
    assert campinas_risco is not None
    assert campinas_risco["id_municipio"] == "3509502"

    assert len(conn.get_fire_hotspots("1501402")) == 2
    assert len(conn.get_seeg_emissions("Energia")) == 12
    assert conn.get_municipal_summary("4106902")["qtd_focos_calor_inpe"] == 48

    emit_top = conn.list_top_municipalities(limit=3, sort_by_emissions=True)
    assert len(emit_top) == 3
    assert emit_top[0]["id_municipio"] == "1501402"

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_inpe_queimadas_seeg_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_queimadas_hotspots("1501402")) == 2
    assert len(sdk.get_seeg_emissions("1501402")) == 2
    assert sdk.get_queimadas_fire_risk("3509502")["id_municipio"] == "3509502"
    assert sdk.get_queimadas_seeg_municipal_summary("4106902")["qtd_focos_calor_inpe"] == 48
    assert sdk.get_queimadas_seeg_features("1501402")["sigla_uf"] == "PA"
    assert len(sdk.list_top_queimadas_seeg_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/environment/queimadas/focos/1501402").status_code == 200
    assert client.get("/api/v1/environment/queimadas/emissoes/1501402").status_code == 200
    assert client.get("/api/v1/environment/queimadas/risco/3509502").status_code == 200
    assert client.get("/api/v1/environment/queimadas/municipio/4106902").status_code == 200
    res_feat = client.get("/api/v1/environment/queimadas/features/1501402")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Belém"
    assert client.get("/api/v1/environment/queimadas/ranking?limit=5&sort_by_emissions=true").status_code == 200

    # 3. CLI
    assert cli_main(["queimadas", "features", "1501402", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1501402"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_inpe_queimadas_focos", {"termo": "1501402"}),
        ("bdg_get_seeg_emissoes_gee", {"termo": "1501402"}),
        ("bdg_get_queimadas_seeg_features", {"id_municipio_or_name": "1501402"}),
        ("bdg_list_top_queimadas_seeg_municipalities", {"limit": 3, "sort_by_emissions": True}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 921,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
