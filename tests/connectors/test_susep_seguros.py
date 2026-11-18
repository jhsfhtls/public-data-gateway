"""Offline tests and 4-interface parity suite for susep_seguros connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.susep_seguros import SusepSegurosConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_susep_seguros_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = SusepSegurosConnector(settings=settings)
    registered = get_connector("susep_seguros", settings=settings)
    assert isinstance(registered, SusepSegurosConnector)
    assert connector.contract.source_id == "susep_seguros"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "susep_seguros" / "silver"
    gold_dir = tmp_path / "susep_seguros" / "gold"
    assert (silver_dir / "susep_premios_sinistros_ramo.parquet").exists()
    assert (silver_dir / "susep_previdencia_vgbl_pgbl_uf.parquet").exists()
    assert (silver_dir / "susep_corretoras_solvencia_mercado.parquet").exists()
    assert (silver_dir / "susep_seguros_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_susep_seguros_brasil.parquet").exists()
    assert (tmp_path / "susep_seguros" / "bronze" / "ingestion_meta.json").exists()

    pr = connector.get_susep_premios_ramo("São Paulo")
    assert len(pr) == 2
    assert pr[0]["grupo_ramo_seguro"] == "AUTO_PATRIMONIAL"

    prev = connector.get_susep_previdencia_vgbl("3550308")
    assert len(prev) == 1
    assert prev[0]["provisoes_tecnicas_mi_brl"] > 100000.0

    corr = connector.get_susep_corretoras_solvencia("Curitiba")
    assert corr is not None
    assert corr["corretoras_ativas_susep"] == 3640

    summary = connector.get_susep_municipal_summary("4106902")
    assert summary is not None
    assert summary["premios_emitidos_total_mil_brl"] == 14650000.0

    features = connector.get_susep_features("São Paulo")
    assert features is not None
    assert features["tier_mercado_segurador_susep"] == "HUB_SEGUROS_SOLVENCIA_ALTA"

    top_sp = connector.list_top_susep_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_susep_seguros_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK FinancialFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_susep_premios_ramo("Campinas")) == 2
    assert len(sdk.get_susep_previdencia_vgbl("3509502")) == 1

    corr = sdk.get_susep_corretoras_solvencia("3509502")
    assert corr is not None
    assert corr["indice_solvencia_regulatoria_pct"] >= 150.0

    summary = sdk.get_susep_municipal_summary("Campinas")
    assert summary is not None
    assert summary["sigla_uf"] == "SP"

    features = sdk.get_susep_features("Campinas")
    assert features is not None
    assert features["score_maturidade_seguros_susep"] >= 78.0

    top_premios = sdk.list_top_susep_municipalities(limit=3, sort_by_premios=True)
    assert len(top_premios) == 3
    assert (
        top_premios[0]["premios_emitidos_total_mil_brl"]
        >= top_premios[1]["premios_emitidos_total_mil_brl"]
    )


def test_susep_seguros_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg susep-seguros CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_pr = client.get("/api/v1/finance/susep-seguros/premios/Goiânia")
    assert r_pr.status_code == 200
    assert len(r_pr.json()) == 2

    r_pv = client.get("/api/v1/finance/susep-seguros/previdencia/5208707")
    assert r_pv.status_code == 200
    assert len(r_pv.json()) == 1

    r_cr = client.get("/api/v1/finance/susep-seguros/corretoras/Goiânia")
    assert r_cr.status_code == 200
    assert r_cr.json()["corretoras_ativas_susep"] > 0

    r_mn = client.get("/api/v1/finance/susep-seguros/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/finance/susep-seguros/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_mercado_segurador_susep"] == "HUB_SEGUROS_SOLVENCIA_ALTA"

    r_rk = client.get("/api/v1/finance/susep-seguros/ranking?limit=3&sort_by_premios=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["susep-seguros", "premios", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["susep-seguros", "previdencia", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["susep-seguros", "corretoras", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["susep-seguros", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["susep-seguros", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_maturidade_seguros_susep"] >= 78.0
    assert cli_main(["susep-seguros", "ranking", "--limit", "3", "--premios", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_susep_seguros_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for SUSEP Seguros."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_susep_premios_ramo",
        "bdg_get_susep_previdencia_vgbl",
        "bdg_get_susep_features",
        "bdg_list_top_susep_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_susep_premios_ramo", {"termo": "Campinas"}),
        (3, "bdg_get_susep_previdencia_vgbl", {"termo": "3509502"}),
        (4, "bdg_get_susep_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_susep_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_premios": True},
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
        assert "susep_seguros" in text
