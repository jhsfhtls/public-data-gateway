"""Deterministic offline tests for BACEN Agência connector and 4-interface parity."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
    BcbAgenciaConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_bcb_agencia_metadata_and_registry(tmp_settings: GatewaySettings) -> None:
    """Validate connector registration, metadata, and ODCS contract loading."""
    conn = BcbAgenciaConnector(settings=tmp_settings)
    assert conn.source_id == "bcb_agencia"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-BCB-AGENCIA-001"
    assert "cnpj_14_agencia" in conn.contract.primary_keys
    registered = get_connector("bcb_agencia", settings=tmp_settings)
    assert isinstance(registered, BcbAgenciaConnector)


def test_bcb_agencia_medallion_silver_and_gold_parquets(
    tmp_settings: GatewaySettings,
) -> None:
    """Validate all 4 Silver Parquets and Gold BACEN capillarity feature store."""
    conn = BcbAgenciaConnector(settings=tmp_settings)
    tab = conn.extract()
    assert tab.num_rows == 24
    assert {
        "cnpj_14_agencia",
        "nome_instituicao",
        "id_municipio",
        "sigla_uf",
    }.issubset(tab.column_names)

    silver_dir = tmp_settings.data_dir / "bcb_agencia" / "silver"
    expected_rows = {
        "agencias_cadastro_nacional.parquet": 24,
        "agencias_postos_atendimento.parquet": 12,
        "agencias_cobertura_municipal.parquet": 12,
        "agencias_cidades_sem_atendimento.parquet": 12,
    }
    for fname, expected in expected_rows.items():
        fpath = silver_dir / fname
        assert fpath.exists(), f"Missing Silver table: {fname}"
        assert pq.read_table(fpath).num_rows == expected

    gold_path = (
        tmp_settings.data_dir
        / "bcb_agencia"
        / "gold"
        / "features_capilaridade_agencias_bcb.parquet"
    )
    assert gold_path.exists()
    gold_rows = pq.read_table(gold_path).to_pylist()
    assert len(gold_rows) == 12
    for row in gold_rows:
        assert len(row["id_municipio"]) == 7
        assert row["total_agencias_fisicas"] > 0
        assert row["classificacao_capilaridade_agencias"] in {
            "ALTA_CAPILARIDADE_BANCARIA",
            "CAPILARIDADE_ADEQUADA",
            "DESERTO_BANCARIO_ALERTA",
        }


def test_bcb_agencia_queries_and_ranking(tmp_settings: GatewaySettings) -> None:
    """Validate branch list, service outposts, coverage, financial inclusion, and ranking."""
    conn = BcbAgenciaConnector(settings=tmp_settings)

    manaus_feats = conn.get_agencia_features("1302603")
    assert manaus_feats is not None
    assert manaus_feats["nome_municipio"] == "Manaus"
    assert manaus_feats["total_agencias_fisicas"] == 148
    assert manaus_feats["classificacao_capilaridade_agencias"] == "CAPILARIDADE_ADEQUADA"

    campinas_cov = conn.get_municipal_coverage("Campinas")
    assert campinas_cov is not None
    assert campinas_cov["id_municipio"] == "3509502"

    assert len(conn.get_branches("1302603")) == 2
    assert conn.get_service_outposts("1302603")["qtd_postos_pae_eletronico"] > 0
    assert conn.get_financial_inclusion_status("1302603")["possui_agencia_fisica"] is True

    sp_top = conn.list_top_municipalities(limit=5, sigla_uf="SP")
    assert len(sp_top) == 2
    assert all(r["sigla_uf"] == "SP" for r in sp_top)


def test_bcb_agencia_four_interface_parity(
    tmp_settings: GatewaySettings, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify full 4-interface parity: SDK, FastAPI, CLI, and MCP with untrusted shielding."""
    # 1. SDK
    sdk = BrazilData(settings=tmp_settings)
    assert len(sdk.get_bcb_agencias("1302603")) == 2
    assert sdk.get_bcb_postos_atendimento("3509502")["id_municipio"] == "3509502"
    assert sdk.get_bcb_cobertura_municipal("1302603")["total_bancos_presentes"] == 15
    assert sdk.get_bcb_inclusao_financeira("1302603")["possui_agencia_fisica"] is True
    assert sdk.get_bcb_agencia_features("3509502")["sigla_uf"] == "SP"
    assert len(sdk.list_top_bcb_agencia_municipalities(limit=3)) == 3

    # 2. FastAPI
    client = TestClient(create_app(tmp_settings))
    assert client.get("/api/v1/banking/agencias/lista/1302603").status_code == 200
    assert client.get("/api/v1/banking/agencias/postos/3509502").status_code == 200
    assert client.get("/api/v1/banking/agencias/cobertura/1302603").status_code == 200
    assert client.get("/api/v1/banking/agencias/inclusao/1302603").status_code == 200
    res_feat = client.get("/api/v1/banking/agencias/features/1302603")
    assert res_feat.status_code == 200
    assert res_feat.json()["nome_municipio"] == "Manaus"
    assert client.get("/api/v1/banking/agencias/ranking?limit=5&uf=SP").status_code == 200

    # 3. CLI
    assert cli_main(["agencias", "features", "1302603", "--json"]) == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["id_municipio"] == "1302603"

    # 4. MCP
    mcp = MCPDispatcher(settings=tmp_settings)
    for tool_name, arguments in [
        ("bdg_get_bcb_agencias_branches", {"id_municipio_or_cnpj": "1302603"}),
        ("bdg_get_bcb_agencias_coverage", {"id_municipio_or_name": "3509502"}),
        ("bdg_get_bcb_agencias_capilaridade_features", {"id_municipio_or_name": "1302603"}),
        ("bdg_list_top_bcb_agencias_municipalities", {"limit": 3, "sigla_uf": "SP"}),
    ]:
        resp = mcp.handle_request({
            "jsonrpc": "2.0",
            "id": 915,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        assert resp.get("error") is None
        assert resp["result"]["isError"] is False
        assert "untrusted_gov_data" in resp["result"]["content"][0]["text"]
