"""Offline tests and 4-interface parity suite for anm_mineracao connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.anm_mineracao import AnmMineracaoConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_anm_mineracao_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = AnmMineracaoConnector(settings=settings)
    registered = get_connector("anm_mineracao", settings=settings)
    assert isinstance(registered, AnmMineracaoConnector)
    assert connector.contract.source_id == "anm_mineracao"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "anm_mineracao" / "silver"
    gold_dir = tmp_path / "anm_mineracao" / "gold"
    assert (silver_dir / "anm_sigmine_processos_municipio.parquet").exists()
    assert (silver_dir / "anm_cfem_arrecadacao_royalties.parquet").exists()
    assert (silver_dir / "anm_sigbm_barragens_risco_esg.parquet").exists()
    assert (silver_dir / "anm_mineracao_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_anm_mineracao_brasil.parquet").exists()
    assert (tmp_path / "anm_mineracao" / "bronze" / "ingestion_meta.json").exists()

    proc = connector.get_anm_sigmine_processes("Parauapebas")
    assert len(proc) == 2
    assert proc[0]["substancia_mineral"] == "FERRO"

    cfem = connector.get_anm_cfem_royalties("1505536")
    assert len(cfem) == 1
    assert cfem[0]["arrecadacao_cfem_municipio_mil_brl"] > 1000000.0

    barragens = connector.get_anm_sigbm_barragens("Itabira")
    assert barragens is not None
    assert barragens["qtd_barragens_sigbm"] == 11

    summary = connector.get_anm_municipal_summary("3131703")
    assert summary is not None
    assert summary["processos_ativos_sigmine"] == 142

    features = connector.get_anm_features("Parauapebas")
    assert features is not None
    assert features["tier_mineracao_sustentavel_anm"] == "POLO_MINERAL_ESG_CONFORME"

    top_mg = connector.list_top_anm_municipalities(limit=5, sigla_uf="MG")
    assert len(top_mg) == 3
    assert all(r["sigla_uf"] == "MG" for r in top_mg)


def test_anm_mineracao_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK EnvironmentFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_anm_sigmine_processes("Catalão")) == 2
    assert len(sdk.get_anm_cfem_royalties("5205109")) == 1

    barragens = sdk.get_anm_sigbm_barragens("5205109")
    assert barragens is not None
    assert barragens["indice_conformidade_sigbm_pct"] >= 90.0

    summary = sdk.get_anm_municipal_summary("Catalão")
    assert summary is not None
    assert summary["sigla_uf"] == "GO"

    features = sdk.get_anm_features("Catalão")
    assert features is not None
    assert features["score_maturidade_mineral_esg"] >= 78.0

    top_cfem = sdk.list_top_anm_municipalities(limit=3, sort_by_cfem=True)
    assert len(top_cfem) == 3
    assert (
        top_cfem[0]["arrecadacao_cfem_total_mil_brl"]
        >= top_cfem[1]["arrecadacao_cfem_total_mil_brl"]
    )


def test_anm_mineracao_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg anm-mineracao CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_proc = client.get("/api/v1/environment/anm-mineracao/processos/Ouro Preto")
    assert r_proc.status_code == 200
    assert len(r_proc.json()) == 2

    r_cfem = client.get("/api/v1/environment/anm-mineracao/cfem/3146107")
    assert r_cfem.status_code == 200
    assert len(r_cfem.json()) == 1

    r_bar = client.get("/api/v1/environment/anm-mineracao/barragens/Ouro Preto")
    assert r_bar.status_code == 200
    assert r_bar.json()["qtd_barragens_sigbm"] == 9

    r_mun = client.get("/api/v1/environment/anm-mineracao/municipio/3146107")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "MG"

    r_ft = client.get("/api/v1/environment/anm-mineracao/features/Ouro Preto")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_mineracao_sustentavel_anm"] == "POLO_MINERAL_ESG_CONFORME"

    r_rk = client.get("/api/v1/environment/anm-mineracao/ranking?limit=3&sort_by_cfem=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["anm-mineracao", "processos", "Corumbá", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["anm-mineracao", "cfem", "5003207", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["anm-mineracao", "barragens", "Corumbá", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "MS"
    assert cli_main(["anm-mineracao", "municipio", "5003207", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5003207"
    assert cli_main(["anm-mineracao", "features", "Corumbá", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_maturidade_mineral_esg"] >= 78.0
    assert cli_main(["anm-mineracao", "ranking", "--limit", "3", "--cfem", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_anm_mineracao_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for ANM Mineração."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_anm_sigmine_processes",
        "bdg_get_anm_cfem_royalties",
        "bdg_get_anm_features",
        "bdg_list_top_anm_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_anm_sigmine_processes", {"termo": "Catalão"}),
        (3, "bdg_get_anm_cfem_royalties", {"termo": "5205109"}),
        (4, "bdg_get_anm_features", {"id_municipio_or_name": "Catalão"}),
        (
            5,
            "bdg_list_top_anm_municipalities",
            {"limit": 3, "sigla_uf": "MG", "sort_by_cfem": True},
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
        assert "anm_mineracao" in text
