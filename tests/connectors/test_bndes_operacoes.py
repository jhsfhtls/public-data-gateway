"""Offline tests and 4-interface parity suite for bndes_operacoes connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
    BndesOperacoesConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_bndes_operacoes_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = BndesOperacoesConnector(settings=settings)
    registered = get_connector("bndes_operacoes", settings=settings)
    assert isinstance(registered, BndesOperacoesConnector)
    assert connector.contract.source_id == "bndes_operacoes"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "bndes_operacoes" / "silver"
    gold_dir = tmp_path / "bndes_operacoes" / "gold"
    assert (silver_dir / "bndes_operacoes_credito_municipio.parquet").exists()
    assert (silver_dir / "bndes_desembolsos_setoriais_uf.parquet").exists()
    assert (silver_dir / "bndes_carteira_risco_garantias.parquet").exists()
    assert (silver_dir / "bndes_operacoes_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_bndes_operacoes_brasil.parquet").exists()
    assert (tmp_path / "bndes_operacoes" / "bronze" / "ingestion_meta.json").exists()

    ops = connector.get_credit_operations("Curitiba")
    assert len(ops) == 2
    assert ops[0]["sigla_uf"] == "PR"

    desemb = connector.get_disbursements("4106902")
    assert len(desemb) == 1
    assert desemb[0]["desembolso_anual_mil_brl"] > 100000.0

    risco = connector.get_portfolio_risk("Curitiba")
    assert risco is not None
    assert risco["cobertura_fgi_garantias_pct"] >= 80.0

    summary = connector.get_bndes_summary("4106902")
    assert summary is not None
    assert summary["valor_total_contratado_mil_brl"] > 1_000_000.0

    features = connector.get_bndes_features("Curitiba")
    assert features is not None
    assert features["tier_maturidade_fomento_bndes"] == "POLO_ALTO_FOMENTO_BNDES"

    top_sp = connector.list_top_bndes_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_bndes_operacoes_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK BankingFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_bndes_operations("Campinas")) == 2
    assert len(sdk.get_bndes_disbursements("3509502")) == 1

    risco = sdk.get_bndes_portfolio_risk("3509502")
    assert risco is not None
    assert risco["inadimplencia_carteira_bndes_pct"] < 1.0

    summary = sdk.get_bndes_municipal_summary("Campinas")
    assert summary is not None
    assert summary["fomento_per_capita_brl"] > 0

    features = sdk.get_bndes_features("Campinas")
    assert features is not None
    assert features["score_dinamismo_fomento_bndes"] >= 75.0

    top_vol = sdk.list_top_bndes_municipalities(limit=3, sort_by_volume=True)
    assert len(top_vol) == 3
    assert top_vol[0]["valor_total_contratado_mil_brl"] >= top_vol[1]["valor_total_contratado_mil_brl"]


def test_bndes_operacoes_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg bndes-operacoes CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_op = client.get("/api/v1/banking/bndes-operacoes/operacoes/Goiânia")
    assert r_op.status_code == 200
    assert len(r_op.json()) == 2

    r_ds = client.get("/api/v1/banking/bndes-operacoes/desembolsos/5208707")
    assert r_ds.status_code == 200
    assert len(r_ds.json()) == 1

    r_rk_m = client.get("/api/v1/banking/bndes-operacoes/risco/Goiânia")
    assert r_rk_m.status_code == 200
    assert r_rk_m.json()["cobertura_fgi_garantias_pct"] > 75.0

    r_mn = client.get("/api/v1/banking/bndes-operacoes/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/banking/bndes-operacoes/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_maturidade_fomento_bndes"] == "POLO_ALTO_FOMENTO_BNDES"

    r_rk = client.get("/api/v1/banking/bndes-operacoes/ranking?limit=3&sort_by_volume=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["bndes-operacoes", "operacoes", "Fortaleza", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["bndes-operacoes", "desembolsos", "2304400", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["bndes-operacoes", "risco", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "CE"
    assert cli_main(["bndes-operacoes", "municipio", "2304400", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "2304400"
    assert cli_main(["bndes-operacoes", "features", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_dinamismo_fomento_bndes"] >= 75.0
    assert cli_main(["bndes-operacoes", "ranking", "--limit", "3", "--volume", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_bndes_operacoes_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for BNDES Operações."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_bndes_operacoes",
        "bdg_get_bndes_desembolsos",
        "bdg_get_bndes_features",
        "bdg_list_top_bndes_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_bndes_operacoes", {"termo": "Campinas"}),
        (3, "bdg_get_bndes_desembolsos", {"termo": "3509502"}),
        (4, "bdg_get_bndes_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_bndes_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_volume": True},
        ),
    ]:
        resp = mcp.handle_request(
            {
                "jsonrpc": "2.0",
                "id": call_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        assert resp["result"]["isError"] is False
        text = resp["result"]["content"][0]["text"]
        assert "bndes_operacoes" in text
