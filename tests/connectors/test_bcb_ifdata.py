"""Deterministic offline tests for the BCB IF.data financial institutions connector and interfaces."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
    BcbIfdataConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_bcb_ifdata_metadata() -> None:
    """Validate connector identifier and contract attributes."""
    conn = BcbIfdataConnector()
    assert conn.source_id == "bcb_ifdata"
    assert conn.test_connection() is True
    assert conn.contract.contract_id == "ODCS-BR-BCB-IFDATA-001"
    assert "cnpj_14_lider" in conn.contract.primary_keys or "cod_inst" in conn.contract.primary_keys


def test_bcb_ifdata_extract_and_parquet_generation(tmp_settings: GatewaySettings) -> None:
    """Validate Medallion layers and Parquet generation for all 5 tables."""
    conn = BcbIfdataConnector(settings=tmp_settings)
    tab = conn.extract()

    assert tab.num_rows >= 10
    assert "cnpj_14_lider" in tab.column_names
    assert "nome_instituicao" in tab.column_names

    silver_dir = tmp_settings.data_dir / "bcb_ifdata" / "silver"
    for name in [
        "bcb_ifdata_cadastro.parquet",
        "bcb_ifdata_ativo.parquet",
        "bcb_ifdata_passivo.parquet",
        "bcb_ifdata_dre.parquet",
        "bcb_ifdata_indicadores.parquet",
    ]:
        fpath = silver_dir / name
        assert fpath.exists(), f"File {name} should exist in silver layer"
        t = pq.read_table(fpath)
        assert t.num_rows > 0


def test_bcb_ifdata_gold_feature_store_metrics(tmp_settings: GatewaySettings) -> None:
    """Validate Gold feature store calculation, continuous banking solidity score and regulatory metrics."""
    conn = BcbIfdataConnector(settings=tmp_settings)
    assert conn.test_connection() is True
    gold_path = tmp_settings.data_dir / "bcb_ifdata" / "gold" / "features_sistema_financeiro_brasil.parquet"
    assert gold_path.exists()

    gold_tab = pq.read_table(gold_path)
    assert gold_tab.num_rows >= 10

    pylist = gold_tab.to_pylist()
    for row in pylist:
        assert len(row["cnpj_14_lider"]) == 14
        assert row["nome_instituicao"]
        assert row["segmento_prudencial"] in {"S1", "S2", "S3", "S4", "S5"}
        assert 0.0 <= row["score_solidez_bancaria"] <= 100.0
        assert row["classificacao_porte_bancario"] in {
            "CONGLOMERADO_S1",
            "BANCO_MEDIO_S2_S3",
            "INSTITUICAO_REGIONAL_S4_S5",
        }
        assert row["ativo_total"] > 0
        assert row["indice_basileia_pct"] > 0
        assert row["indice_imobilizacao_pct"] >= 0


def test_bcb_ifdata_queries_and_filters(tmp_settings: GatewaySettings) -> None:
    """Validate connector lookup methods by CNPJ, balance sheet and ranking."""
    conn = BcbIfdataConnector(settings=tmp_settings)

    # 1. Get bank by CNPJ
    bank = conn.get_bank_by_cnpj("60701190000104")
    assert bank is not None
    assert "ITAU UNIBANCO" in bank["nome_instituicao"]
    assert bank["segmento_prudencial"] == "S1"

    # 2. Balance sheet
    sheet = conn.get_bank_balance_sheet("60701190000104")
    assert sheet is not None
    assert sheet["instituicao"]["nome_instituicao"] == bank["nome_instituicao"]
    assert sheet["ativo"]["ativo_total"] > 1_000_000_000_000.0
    assert sheet["passivo_e_pl"]["patrimonio_liquido"] > 0

    # 3. Ranking total and filtered
    top5 = conn.get_banking_ranking(top=5)
    assert len(top5) == 5
    assert top5[0]["ativo_total"] >= top5[1]["ativo_total"]

    s1_banks = conn.get_banking_ranking(segmento="S1")
    for b in s1_banks:
        assert b["segmento_prudencial"] == "S1"


def test_bcb_ifdata_sdk_facade(tmp_settings: GatewaySettings) -> None:
    """Validate Python SDK facade exposure via BrazilData."""
    sdk = BrazilData(settings=tmp_settings)

    # Via facade
    bank = sdk.banking.get_bank_by_cnpj("00000000000191")
    assert bank is not None
    assert "BANCO DO BRASIL" in bank["nome_instituicao"]

    # Via shortcut
    bank_shortcut = sdk.get_bank_profile("00000000000191")
    assert bank_shortcut is not None
    assert bank_shortcut["cod_inst"] == bank["cod_inst"]

    # Balance and Solidity
    bal = sdk.banking.get_bank_balance_sheet("00000000000191")
    assert bal is not None
    assert bal["ativo"]["ativo_total"] > 0

    sol = sdk.get_bank_solidity("00000000000191")
    assert sol is not None
    assert 0.0 <= sol["score_solidez_bancaria"] <= 100.0


def test_bcb_ifdata_cli_commands(tmp_settings: GatewaySettings, capsys: any) -> None:
    """Validate CLI execution for banking commands and ifdata shortcut."""
    # 1. pdg banking profile
    exit_code = cli_main(["banking", "profile", "60701190000104", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "ITAU" in data["nome_instituicao"]

    # 2. pdg banking balance
    exit_code = cli_main(["banking", "balance", "60701190000104", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    sheet = json.loads(captured.out)
    assert "ativo" in sheet
    assert sheet["ativo"]["ativo_total"] > 0

    # 3. pdg banking ranking
    exit_code = cli_main(["banking", "ranking", "--top", "3", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    ranking = json.loads(captured.out)
    assert len(ranking) == 3

    # 4. pdg ifdata shortcut
    exit_code = cli_main(["ifdata", "60701190000104", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    shortcut_data = json.loads(captured.out)
    assert "instituicao" in shortcut_data
    assert "solidez" in shortcut_data


def test_bcb_ifdata_api_endpoints(tmp_settings: GatewaySettings) -> None:
    """Validate REST API endpoints under /api/v1/banking and /api/v1/ifdata."""
    app = create_app(tmp_settings)
    client = TestClient(app)

    # 1. Banking profile
    res = client.get("/api/v1/banking/profile/60701190000104")
    assert res.status_code == 200
    assert "ITAU" in res.json()["nome_instituicao"]

    # 2. Banking balance
    res_bal = client.get("/api/v1/banking/balance/60701190000104")
    assert res_bal.status_code == 200
    assert res_bal.json()["ativo"]["ativo_total"] > 0

    # 3. Banking ranking
    res_rank = client.get("/api/v1/banking/ranking?top=5&segmento=S1")
    assert res_rank.status_code == 200
    assert len(res_rank.json()) <= 5
    for item in res_rank.json():
        assert item["segmento_prudencial"] == "S1"

    # 4. Banking solidity
    res_sol = client.get("/api/v1/banking/solidity/60701190000104")
    assert res_sol.status_code == 200
    assert res_sol.json()["score_solidez_bancaria"] > 0


def test_bcb_ifdata_mcp_tools(tmp_settings: GatewaySettings) -> None:
    """Validate MCP JSON-RPC tool handling with prompt injection shielding."""
    dispatcher = MCPDispatcher(settings=tmp_settings)

    # 1. bdg_get_bank_financial_profile
    res = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 101,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_bank_financial_profile",
            "arguments": {"cnpj": "60701190000104"},
        },
    })
    assert res.get("error") is None
    assert res["result"]["isError"] is False
    text = res["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in text
    assert "ITAU" in text

    # 2. bdg_get_banking_ranking
    res_rank = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 102,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_banking_ranking",
            "arguments": {"segmento": "S1", "top": 4},
        },
    })
    assert res_rank.get("error") is None
    assert res_rank["result"]["isError"] is False
    text_rank = res_rank["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in text_rank
