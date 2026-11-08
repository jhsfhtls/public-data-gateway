"""Deterministic offline tests for the STN Tesouro Transparente connector and interfaces."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
    StnTesouroTransparenteConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_stn_tesouro_metadata() -> None:
    """Validate connector identifier and contract attributes."""
    conn = StnTesouroTransparenteConnector()
    assert conn.source_id == "stn_tesouro_transparente"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-STN-TESOURO-001"
    assert "id_municipio_ibge_7d" in conn.contract.primary_keys or "sigla_uf" in conn.contract.primary_keys


def test_stn_tesouro_extract_and_parquet_generation(tmp_settings: GatewaySettings) -> None:
    """Validate Medallion layers and Parquet generation for all 5 tables."""
    conn = StnTesouroTransparenteConnector(settings=tmp_settings)
    tab = conn.extract()

    assert tab.num_rows >= 10
    assert "id_pleito" in tab.column_names
    assert "interessado" in tab.column_names

    silver_dir = tmp_settings.data_dir / "stn_tesouro_transparente" / "silver"
    for name in [
        "stn_sadipem_pvl.parquet",
        "stn_transferencias_fpm.parquet",
        "stn_transferencias_fpe.parquet",
        "stn_capag_municipios.parquet",
        "stn_divida_consolidada.parquet",
    ]:
        fpath = silver_dir / name
        assert fpath.exists(), f"File {name} should exist in silver layer"
        t = pq.read_table(fpath)
        assert t.num_rows > 0


def test_stn_tesouro_gold_feature_store_metrics(tmp_settings: GatewaySettings) -> None:
    """Validate Gold feature store calculation, continuous fiscal score and CAPAG ratings."""
    conn = StnTesouroTransparenteConnector(settings=tmp_settings)
    assert conn.test_connection() is True
    gold_path = tmp_settings.data_dir / "stn_tesouro_transparente" / "gold" / "features_fiscal_municipios_brasil.parquet"
    assert gold_path.exists()

    gold_tab = pq.read_table(gold_path)
    assert gold_tab.num_rows >= 10

    pylist = gold_tab.to_pylist()
    for row in pylist:
        assert len(row["id_municipio_ibge_7d"]) == 7
        assert row["nome_municipio"]
        assert row["nota_capag_final"] in {"A", "B", "C"}
        assert 0.0 <= row["score_saude_fiscal"] <= 100.0
        assert row["classificacao_saude_fiscal"] in {
            "EXCELENTE_CAPAG_A",
            "REGULAR_CAPAG_B",
            "CRITICO_CAPAG_C",
        }
        assert row["receita_corrente_liquida_rcl"] > 0
        assert row["total_fpm_anual"] > 0


def test_stn_tesouro_queries_and_filters(tmp_settings: GatewaySettings) -> None:
    """Validate connector lookup methods by municipality, PVL credit operations and FPM transfers."""
    conn = StnTesouroTransparenteConnector(settings=tmp_settings)

    # 1. Get fiscal profile by IBGE ID
    prof = conn.get_municipio_fiscal_profile("3550308")
    assert prof is not None
    assert "SÃO PAULO" in prof["nome_municipio"].upper()
    assert prof["nota_capag_final"] == "A"

    # 2. Get PVL credit operations
    pvls = conn.get_credit_operations_pvl("3550308")
    assert len(pvls) > 0
    assert pvls[0]["valor_operacao"] > 0

    # 3. Get FPM transfers
    fpms = conn.get_fpm_transfers("3550308", ano=2024)
    assert len(fpms) == 36  # 12 months * 3 decendios
    assert fpms[0]["valor_bruto_fpm"] > 0
    assert fpms[0]["deducao_fundeb"] > 0

    # 4. Get CAPAG rating
    cap = conn.get_capag_rating("3550308")
    assert cap is not None
    assert cap["nota_capag_final"] == "A"


def test_stn_tesouro_sdk_facade(tmp_settings: GatewaySettings) -> None:
    """Validate Python SDK facade exposure via BrazilData."""
    sdk = BrazilData(settings=tmp_settings)

    # Via facade
    prof = sdk.treasury.get_municipio_fiscal_profile("3106200")
    assert prof is not None
    assert "BELO HORIZONTE" in prof["nome_municipio"].upper()

    # Via shortcut
    prof_shortcut = sdk.get_municipio_fiscal_profile("3106200")
    assert prof_shortcut is not None
    assert prof_shortcut["id_municipio_ibge_7d"] == prof["id_municipio_ibge_7d"]

    # Credit operations & CAPAG
    pvls = sdk.get_credit_operations_pvl("3106200")
    assert len(pvls) > 0

    cap = sdk.get_capag_rating("3106200")
    assert cap is not None
    assert cap["nota_capag_final"] in ("A", "B", "C")


def test_stn_tesouro_cli_commands(tmp_settings: GatewaySettings, capsys: any) -> None:
    """Validate CLI execution for treasury commands and sadipem shortcut."""
    # 1. pdg treasury profile
    exit_code = cli_main(["treasury", "profile", "3550308", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "PAULO" in data["nome_municipio"].upper()

    # 2. pdg treasury pvl
    exit_code = cli_main(["treasury", "pvl", "3550308", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    pvls = json.loads(captured.out)
    assert len(pvls) > 0

    # 3. pdg treasury fpm
    exit_code = cli_main(["treasury", "fpm", "3550308", "--ano", "2024", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    fpms = json.loads(captured.out)
    assert len(fpms) > 0

    # 4. pdg treasury capag
    exit_code = cli_main(["treasury", "capag", "3550308", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    cap = json.loads(captured.out)
    assert cap["nota_capag_final"] == "A"

    # 5. pdg sadipem shortcut
    exit_code = cli_main(["sadipem", "3550308", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    shortcut_data = json.loads(captured.out)
    assert "perfil_fiscal" in shortcut_data
    assert "operacoes_credito" in shortcut_data


def test_stn_tesouro_api_endpoints(tmp_settings: GatewaySettings) -> None:
    """Validate REST API endpoints under /api/v1/treasury and /api/v1/sadipem."""
    app = create_app(tmp_settings)
    client = TestClient(app)

    # 1. Treasury profile
    res = client.get("/api/v1/treasury/profile/3550308")
    assert res.status_code == 200
    assert "PAULO" in res.json()["nome_municipio"].upper()

    # 2. SADIPEM PVL
    res_pvl = client.get("/api/v1/treasury/pvl/3550308")
    assert res_pvl.status_code == 200
    assert len(res_pvl.json()) > 0

    # 3. FPM transfers
    res_fpm = client.get("/api/v1/treasury/fpm/3550308?ano=2024")
    assert res_fpm.status_code == 200
    assert len(res_fpm.json()) > 0

    # 4. CAPAG rating
    res_cap = client.get("/api/v1/treasury/capag/3550308")
    assert res_cap.status_code == 200
    assert res_cap.json()["nota_capag_final"] == "A"


def test_stn_tesouro_mcp_tools(tmp_settings: GatewaySettings) -> None:
    """Validate MCP JSON-RPC tool handling with prompt injection shielding."""
    dispatcher = MCPDispatcher(settings=tmp_settings)

    # 1. bdg_get_municipality_fiscal_health
    res = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 201,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_municipality_fiscal_health",
            "arguments": {"id_municipio_or_name": "3550308"},
        },
    })
    assert res.get("error") is None
    assert res["result"]["isError"] is False
    text = res["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in text
    assert "PAULO" in text.upper()

    # 2. bdg_get_sadipem_credit_operations
    res_pvl = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 202,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_sadipem_credit_operations",
            "arguments": {"termo": "3550308"},
        },
    })
    assert res_pvl.get("error") is None
    assert res_pvl["result"]["isError"] is False
    text_pvl = res_pvl["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in text_pvl
