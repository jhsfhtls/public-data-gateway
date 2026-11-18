"""Offline tests and 4-interface parity suite for aneel_geracao_distribuida connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
    AneelGeracaoDistribuidaConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_aneel_mmgd_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = AneelGeracaoDistribuidaConnector(settings=settings)
    registered = get_connector("aneel_geracao_distribuida", settings=settings)
    assert isinstance(registered, AneelGeracaoDistribuidaConnector)
    assert connector.contract.source_id == "aneel_geracao_distribuida"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "aneel_geracao_distribuida" / "silver"
    gold_dir = tmp_path / "aneel_geracao_distribuida" / "gold"
    assert (silver_dir / "aneel_empreendimentos_mmgd.parquet").exists()
    assert (silver_dir / "aneel_geracao_fonte_municipio.parquet").exists()
    assert (silver_dir / "aneel_distribuidoras_rede_mmgd.parquet").exists()
    assert (silver_dir / "aneel_mmgd_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_transicao_energetica_mmgd.parquet").exists()
    assert (tmp_path / "aneel_geracao_distribuida" / "bronze" / "ingestion_meta.json").exists()

    plants = connector.get_mmgd_plants("Cuiabá")
    assert len(plants) >= 2
    assert all(p["sigla_uf"] == "MT" for p in plants)

    sources = connector.get_mmgd_sources_by_municipality("5103403")
    assert len(sources) == 2
    assert any(s["fonte_geracao"] == "Solar_Fotovoltaica" for s in sources)

    utility = connector.get_mmgd_utility_grid("Cuiaba")
    assert utility is not None
    assert utility["distribuidora_principal"] == "Energisa_MT"

    summary = connector.get_mmgd_municipal_summary("5103403")
    assert summary is not None
    assert summary["qtd_usinas_mmgd"] > 0

    features = connector.get_mmgd_features("Cuiabá")
    assert features is not None
    assert features["tier_maturidade_solar_mmgd"] == "POLO_SOLAR_MMGD_AVANCADO"

    top_mt = connector.list_top_mmgd_municipalities(limit=3, sigla_uf="MT")
    assert len(top_mt) == 1
    assert top_mt[0]["id_municipio"] == "5103403"


def test_aneel_mmgd_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK EnvironmentFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_mmgd_plants("Goiânia")) >= 2
    assert len(sdk.get_mmgd_sources_by_municipality("GO")) >= 2
    grid = sdk.get_mmgd_utility_grid("5208707")
    assert grid is not None
    assert grid["distribuidora_principal"] == "Equatorial_GO"

    summary = sdk.get_mmgd_municipal_summary("Goiânia")
    assert summary is not None
    assert summary["id_municipio"] == "5208707"

    features = sdk.get_mmgd_features("5208707")
    assert features is not None
    assert features["score_transicao_energetica_mmgd"] >= 75.0

    top = sdk.list_top_mmgd_municipalities(limit=3, sort_by_power=True)
    assert len(top) == 3
    assert top[0]["potencia_instalada_total_kw"] >= top[1]["potencia_instalada_total_kw"]


def test_aneel_mmgd_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg mmgd CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_emp = client.get("/api/v1/environment/mmgd/empreendimentos/Campinas")
    assert r_emp.status_code == 200
    assert len(r_emp.json()) >= 2

    r_fnt = client.get("/api/v1/environment/mmgd/fontes/3509502")
    assert r_fnt.status_code == 200
    assert len(r_fnt.json()) == 2

    r_dst = client.get("/api/v1/environment/mmgd/distribuidora/Campinas")
    assert r_dst.status_code == 200
    assert r_dst.json()["distribuidora_principal"] == "CPFL_Paulista"

    r_mun = client.get("/api/v1/environment/mmgd/municipio/3509502")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "SP"

    r_ft = client.get("/api/v1/environment/mmgd/features/Campinas")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_maturidade_solar_mmgd"] == "POLO_SOLAR_MMGD_AVANCADO"

    r_rk = client.get("/api/v1/environment/mmgd/ranking?limit=3&sort_by_power=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["mmgd", "empreendimentos", "Curitiba", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) >= 2
    assert cli_main(["mmgd", "fontes", "4106902", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["mmgd", "distribuidora", "Curitiba", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["distribuidora_principal"] == "Copel_DIS"
    assert cli_main(["mmgd", "municipio", "4106902", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "4106902"
    assert cli_main(["mmgd", "features", "Curitiba", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_transicao_energetica_mmgd"] >= 75.0
    assert cli_main(["mmgd", "ranking", "--limit", "3", "--potencia", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_aneel_mmgd_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for ANEEL MMGD."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_aneel_mmgd_empreendimentos",
        "bdg_get_aneel_mmgd_fontes",
        "bdg_get_aneel_mmgd_features",
        "bdg_list_top_aneel_mmgd_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_aneel_mmgd_empreendimentos", {"termo": "Uberlândia"}),
        (3, "bdg_get_aneel_mmgd_fontes", {"termo": "3170206"}),
        (4, "bdg_get_aneel_mmgd_features", {"id_municipio_or_name": "Uberlândia"}),
        (
            5,
            "bdg_list_top_aneel_mmgd_municipalities",
            {"limit": 3, "sigla_uf": "MG", "sort_by_power": True},
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
        assert "aneel_geracao_distribuida" in text
