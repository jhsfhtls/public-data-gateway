"""Deterministic offline tests for IBAMA SIFISC connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
    IbamaFiscalizacaoConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_ibama_fiscalizacao_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = IbamaFiscalizacaoConnector(settings=tmp_settings)
    assert conn.source_id == "ibama_fiscalizacao"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-IBAMA-FISCALIZACAO-001"
    assert "num_auto_infracao" in conn.contract.primary_keys
    registered = get_connector("ibama_fiscalizacao", settings=tmp_settings)
    assert isinstance(registered, IbamaFiscalizacaoConnector)


def test_ibama_fiscalizacao_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold IBAMA enforcement risk feature store."""
    conn = IbamaFiscalizacaoConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "num_auto_infracao",
        "documento_infrator",
        "valor_multa_brl",
        "id_municipio",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "ibama_fiscalizacao" / "silver"
    expected_rows = {
        "ibama_autos_infracao.parquet": 24,
        "ibama_termos_embargo.parquet": 24,
        "ibama_julgamentos_multas.parquet": 12,
        "ibama_resumo_municipal.parquet": 12,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "ibama_fiscalizacao"
        / "gold"
        / "features_risco_ambiental_ibama.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["qtd_autos_infracao_ibama"] > 0
        assert row["classificacao_risco_ibama"] in {
            "BAIXA_PRESSAO_AMBIENTAL",
            "ATENCAO_FISCALIZACAO_MODERADA",
            "CRITICO_EMBARGO_IBAMA_EUDR",
        }


def test_ibama_fiscalizacao_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate infraction notices, embargoes, fines judgment, municipal summary, and ranking."""
    conn = IbamaFiscalizacaoConnector(settings=tmp_settings)

    belem_feats = conn.get_ibama_features("1501402")
    assert belem_feats is not None
    assert belem_feats["nome_municipio"] == "Belém"
    assert belem_feats["classificacao_risco_ibama"] == "CRITICO_EMBARGO_IBAMA_EUDR"

    campinas_multas = conn.get_fines_judgment("Campinas")
    assert campinas_multas is not None
    assert campinas_multas["id_municipio"] == "3509502"

    assert len(conn.get_infractions("1302603")) == 2
    assert len(conn.get_embargoes("Amazonia")) == 4
    assert conn.get_municipal_summary("4106902")["qtd_embargos_ativos_ibama"] == 4

    risk_top = conn.list_top_municipalities(limit=3, sort_by_risk=True)
    assert len(risk_top) == 3
    assert risk_top[0]["id_municipio"] == "1501402"

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_ibama_fiscalizacao_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_ibama_infractions("1302603")) == 2
    assert len(sdk.get_ibama_embargoes("1501402")) == 2
    assert sdk.get_ibama_fines_judgment("3509502")["id_municipio"] == "3509502"
    assert sdk.get_ibama_municipal_summary("4106902")["qtd_embargos_ativos_ibama"] == 4
    assert sdk.get_ibama_features("1501402")["sigla_uf"] == "PA"
    assert len(sdk.list_top_ibama_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/environment/ibama/autos/1302603").status_code == 200
    assert client.get("/api/v1/environment/ibama/embargos/1501402").status_code == 200
    assert client.get("/api/v1/environment/ibama/multas/3509502").status_code == 200
    assert client.get("/api/v1/environment/ibama/municipio/4106902").status_code == 200
    res_feat = client.get("/api/v1/environment/ibama/features/1501402")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Belém"
    assert client.get("/api/v1/environment/ibama/ranking?limit=5&sort_by_risk=true").status_code == 200

    # 3. CLI
    assert cli_main(["ibama", "features", "1501402", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1501402"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_ibama_infractions", {"termo": "1302603"}),
        ("bdg_get_ibama_embargoes", {"termo": "1501402"}),
        ("bdg_get_ibama_enforcement_features", {"id_municipio_or_name": "1501402"}),
        ("bdg_list_top_ibama_municipalities", {"limit": 3, "sort_by_risk": True}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 917,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
