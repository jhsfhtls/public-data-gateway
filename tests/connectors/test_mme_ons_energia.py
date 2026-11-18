"""Offline tests and 4-interface parity suite for mme_ons_energia connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.mme_ons_energia import MmeOnsEnergiaConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_mme_ons_energia_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = MmeOnsEnergiaConnector(settings=settings)
    registered = get_connector("mme_ons_energia", settings=settings)
    assert isinstance(registered, MmeOnsEnergiaConnector)
    assert connector.contract.source_id == "mme_ons_energia"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "mme_ons_energia" / "silver"
    gold_dir = tmp_path / "mme_ons_energia" / "gold"
    assert (silver_dir / "ons_capacidade_geracao_municipio.parquet").exists()
    assert (silver_dir / "ons_balanco_carga_subsistema_uf.parquet").exists()
    assert (silver_dir / "mme_intercambio_seguranca_energetica.parquet").exists()
    assert (silver_dir / "ons_energia_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_seguranca_energetica_ons.parquet").exists()
    assert (tmp_path / "mme_ons_energia" / "bronze" / "ingestion_meta.json").exists()

    ger = connector.get_ons_generation_capacity("Curitiba")
    assert len(ger) == 2
    assert ger[0]["subsistema_sin"] == "SUL"

    bal = connector.get_ons_load_balance("4106902")
    assert len(bal) == 1
    assert bal[0]["ear_energia_armazenada_pct"] > 0

    grid = connector.get_mme_grid_security("Curitiba")
    assert grid is not None
    assert grid["subestacoes_alta_tensao"] > 0

    summary = connector.get_ons_municipal_summary("4106902")
    assert summary is not None
    assert summary["potencia_instalada_sin_mw"] == 1120.0

    features = connector.get_ons_features("Curitiba")
    assert features is not None
    assert features["tier_seguranca_energetica_sin"] == "ALTA_SEGURANCA_ENERGETICA_SIN"

    top_sp = connector.list_top_ons_energy_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_mme_ons_energia_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK EnvironmentFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_ons_generation_capacity("Campinas")) == 2
    assert len(sdk.get_ons_load_balance("3509502")) == 1

    grid = sdk.get_mme_grid_security("3509502")
    assert grid is not None
    assert grid["disponibilidade_rede_sin_pct"] >= 99.0

    summary = sdk.get_ons_municipal_summary("Campinas")
    assert summary is not None
    assert summary["subsistema_sin"] == "SUDESTE_CENTRO_OESTE"

    features = sdk.get_ons_features("Campinas")
    assert features is not None
    assert features["score_seguranca_energetica_ons"] >= 75.0

    top_ren = sdk.list_top_ons_energy_municipalities(limit=3, sort_by_renewable=True)
    assert len(top_ren) == 3
    assert top_ren[0]["geracao_renovavel_sin_pct"] >= top_ren[1]["geracao_renovavel_sin_pct"]


def test_mme_ons_energia_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg sistema-eletrico CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_ger = client.get("/api/v1/environment/sistema-eletrico/geracao/Goiânia")
    assert r_ger.status_code == 200
    assert len(r_ger.json()) == 2

    r_bal = client.get("/api/v1/environment/sistema-eletrico/balanco/5208707")
    assert r_bal.status_code == 200
    assert len(r_bal.json()) == 1

    r_red = client.get("/api/v1/environment/sistema-eletrico/rede/Goiânia")
    assert r_red.status_code == 200
    assert r_red.json()["subestacoes_alta_tensao"] > 0

    r_mun = client.get("/api/v1/environment/sistema-eletrico/municipio/5208707")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/environment/sistema-eletrico/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_seguranca_energetica_sin"] == "ALTA_SEGURANCA_ENERGETICA_SIN"

    r_rk = client.get("/api/v1/environment/sistema-eletrico/ranking?limit=3&sort_by_renewable=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["sistema-eletrico", "geracao", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["sistema-eletrico", "balanco", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["sistema-eletrico", "rede", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["sistema-eletrico", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["sistema-eletrico", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_seguranca_energetica_ons"] >= 75.0
    assert cli_main(["sistema-eletrico", "ranking", "--limit", "3", "--renovavel", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_mme_ons_energia_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for MME/ONS Energia."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_mme_ons_geracao_sin",
        "bdg_get_mme_ons_balanco_carga",
        "bdg_get_mme_ons_features",
        "bdg_list_top_mme_ons_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_mme_ons_geracao_sin", {"termo": "Campinas"}),
        (3, "bdg_get_mme_ons_balanco_carga", {"termo": "3509502"}),
        (4, "bdg_get_mme_ons_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_mme_ons_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_renewable": True},
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
        assert "mme_ons_energia" in text
