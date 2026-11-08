"""Deterministic offline tests for the CVM Fundos de Investimento connector and interfaces."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
    CvmFundosConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_cvm_fundos_metadata() -> None:
    """Validate connector identifier and contract attributes."""
    conn = CvmFundosConnector()
    assert conn.source_id == "cvm_fundos"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-CVM-FUNDOS-001"
    assert "cnpj_14" in conn.contract.primary_keys


def test_cvm_fundos_extract_and_parquet_generation(tmp_settings: GatewaySettings) -> None:
    """Validate Medallion layers and Parquet generation for all 4 tables."""
    conn = CvmFundosConnector(settings=tmp_settings)
    tab = conn.extract()

    assert tab.num_rows >= 5
    assert "cnpj_14" in tab.column_names
    assert "razao_social" in tab.column_names

    silver_dir = tmp_settings.data_dir / "cvm_fundos" / "silver"
    for name in [
        "cvm_cadastro_fundos.parquet",
        "cvm_informe_diario.parquet",
        "cvm_gestores_fundos.parquet",
        "cvm_administradores_fundos.parquet",
    ]:
        fpath = silver_dir / name
        assert fpath.exists(), f"File {name} should exist in silver layer"
        t = pq.read_table(fpath)
        assert t.num_rows > 0


def test_cvm_fundos_gold_feature_store_metrics(tmp_settings: GatewaySettings) -> None:
    """Validate Gold feature store calculation, continuous resilience score and liquidity classes."""
    conn = CvmFundosConnector(settings=tmp_settings)
    assert conn.test_connection() is True
    gold_path = tmp_settings.data_dir / "cvm_fundos" / "gold" / "features_fundos_investimento_brasil.parquet"
    assert gold_path.exists()

    gold_tab = pq.read_table(gold_path)
    assert gold_tab.num_rows >= 5

    pylist = gold_tab.to_pylist()
    for row in pylist:
        assert len(row["cnpj_14"]) == 14
        assert row["razao_social"]
        assert row["classe_cvm"] in {"Multimercado", "Renda Fixa", "Ações", "Cambial"}
        assert 0.0 <= row["score_resiliencia_fundo"] <= 100.0
        assert row["classificacao_liquidez"] in {"ALTA_LIQUIDEZ", "MEDIA_LIQUIDEZ", "RESTRITA"}
        assert row["patrimonio_liquido_atual"] > 0
        assert row["valor_cota_atual"] > 0


def test_cvm_fundos_queries_and_filters(tmp_settings: GatewaySettings) -> None:
    """Validate connector lookup methods by CNPJ, date range and AuM ranking."""
    conn = CvmFundosConnector(settings=tmp_settings)

    # 1. Get fund by CNPJ
    fund = conn.get_fund_by_cnpj("03243208000158")
    assert fund is not None
    assert "KINEA CHRONOS" in fund["razao_social"]

    # 2. Daily reports with date filter
    reports = conn.get_daily_reports("03243208000158", start_date="2024-09-02", end_date="2024-09-10")
    assert len(reports) > 0
    for r in reports:
        assert r["cnpj_14"] == "03243208000158"
        assert "2024-09-02" <= r["data_competencia"] <= "2024-09-10"

    # 3. Asset managers ranking
    managers = conn.get_asset_managers(query="Kinea")
    assert len(managers) >= 1
    assert "KINEA" in managers[0]["nome_gestor"].upper()


def test_cvm_fundos_sdk_facade(tmp_settings: GatewaySettings) -> None:
    """Validate Python SDK facade exposure via BrazilData."""
    sdk = BrazilData(settings=tmp_settings)

    # Via facade
    fund = sdk.funds.get_fund_by_cnpj("11182064000177")
    assert fund is not None
    assert "SPX NIMITZ" in fund["razao_social"]

    # Via shortcut
    fund_shortcut = sdk.get_fund_profile("11182064000177")
    assert fund_shortcut is not None
    assert fund_shortcut["codigo_cvm"] == fund["codigo_cvm"]

    # Features
    feat = sdk.get_fund_features("11182064000177")
    assert feat is not None
    assert 0.0 <= feat["score_resiliencia_fundo"] <= 100.0


def test_cvm_fundos_cli_commands(tmp_settings: GatewaySettings, capsys: any) -> None:
    """Validate CLI execution for funds commands and shortcut."""
    # 1. pdg funds profile
    exit_code = cli_main(["funds", "profile", "00017024000153", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["cnpj_14"] == "00017024000153"

    # 2. pdg funds daily
    exit_code = cli_main(["funds", "daily", "00017024000153", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    daily_data = json.loads(captured.out)
    assert len(daily_data) > 0

    # 3. pdg funds managers
    exit_code = cli_main(["funds", "managers", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    mgr_data = json.loads(captured.out)
    assert len(mgr_data) > 0

    # 4. Shortcut pdg cvm-fundos
    exit_code = cli_main(["cvm-fundos", "09143435000160", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    btg_data = json.loads(captured.out)
    assert "BTG PACTUAL" in btg_data["razao_social"]


def test_cvm_fundos_rest_api(tmp_settings: GatewaySettings) -> None:
    """Validate FastAPI REST endpoints for CVM Fundos."""
    app = create_app(settings=tmp_settings)
    client = TestClient(app)

    # 1. Profile endpoint
    resp = client.get("/api/v1/funds/profile/03243208000158")
    assert resp.status_code == 200
    assert "KINEA" in resp.json()["razao_social"]

    # 2. Daily endpoint
    resp = client.get("/api/v1/funds/daily/03243208000158")
    assert resp.status_code == 200
    assert len(resp.json()) > 0

    # 3. Managers endpoint
    resp = client.get("/api/v1/funds/managers?query=Itau")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1

    # 4. Features endpoint
    resp = client.get("/api/v1/funds/features/03243208000158")
    assert resp.status_code == 200
    assert 0.0 <= resp.json()["score_resiliencia_fundo"] <= 100.0


def test_cvm_fundos_mcp_tools(tmp_settings: GatewaySettings) -> None:
    """Validate MCP tools for fund profile and asset managers with injection containment."""
    dispatcher = MCPDispatcher(settings=tmp_settings)

    # 1. Tools list contains fund tools
    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    assert "bdg_get_fund_profile" in tool_names
    assert "bdg_get_asset_manager_funds" in tool_names

    # 2. Call bdg_get_fund_profile
    req_fund = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_fund_profile",
            "arguments": {"cnpj": "28615186000181"},
        },
    }
    res_fund = dispatcher.handle_request(req_fund)
    assert res_fund["result"]["isError"] is False
    text = res_fund["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in text
    assert "BRADESCO" in text
