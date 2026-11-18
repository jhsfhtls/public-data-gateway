"""Offline tests and 4-interface parity suite for anp_combustiveis connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.anp_combustiveis import (
    AnpCombustiveisConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_anp_combustiveis_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = AnpCombustiveisConnector(settings=settings)
    registered = get_connector("anp_combustiveis", settings=settings)
    assert isinstance(registered, AnpCombustiveisConnector)
    assert connector.contract.source_id == "anp_combustiveis"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "anp_combustiveis" / "silver"
    gold_dir = tmp_path / "anp_combustiveis" / "gold"
    assert (silver_dir / "anp_precos_revenda_municipio.parquet").exists()
    assert (silver_dir / "anp_vendas_derivados_estado.parquet").exists()
    assert (silver_dir / "anp_postos_revendedores_resumo.parquet").exists()
    assert (silver_dir / "anp_combustiveis_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_combustiveis_biocombustiveis_anp.parquet").exists()
    assert (tmp_path / "anp_combustiveis" / "bronze" / "ingestion_meta.json").exists()

    prices = connector.get_anp_fuel_prices("Campinas")
    assert len(prices) == 3
    assert all(p["sigla_uf"] == "SP" for p in prices)

    sales = connector.get_anp_fuel_sales("3509502")
    assert len(sales) == 2
    assert any(s["categoria_combustivel"] == "Etanol_Biodiesel_B14" for s in sales)

    retail = connector.get_anp_retail_network("Campinas")
    assert retail is not None
    assert retail["qtd_postos_ativos_anp"] > 0

    summary = connector.get_anp_municipal_summary("3509502")
    assert summary is not None
    assert summary["preco_diesel_s10_brl"] > 0

    features = connector.get_anp_features("Campinas")
    assert features is not None
    assert features["tier_matriz_combustiveis_anp"] == "POLO_BIOCOMBUSTIVEL_COMPETITIVO"

    top_mt = connector.list_top_anp_municipalities(limit=3, sigla_uf="MT")
    assert len(top_mt) == 1
    assert top_mt[0]["id_municipio"] == "5103403"


def test_anp_combustiveis_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK EnvironmentFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_anp_fuel_prices("Goiânia")) == 3
    assert len(sdk.get_anp_fuel_sales("GO")) == 2
    retail = sdk.get_anp_retail_network("5208707")
    assert retail is not None
    assert retail["taxa_conformidade_pmqc_pct"] >= 90.0

    summary = sdk.get_anp_municipal_summary("Goiânia")
    assert summary is not None
    assert summary["id_municipio"] == "5208707"

    features = sdk.get_anp_features("5208707")
    assert features is not None
    assert features["score_competitividade_combustiveis_bio"] >= 75.0

    top = sdk.list_top_anp_municipalities(limit=3, sort_by_biofuel=True)
    assert len(top) == 3
    assert top[0]["participacao_biocombustivel_pct"] >= top[1]["participacao_biocombustivel_pct"]


def test_anp_combustiveis_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg combustiveis CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_pr = client.get("/api/v1/environment/combustiveis/precos/Curitiba")
    assert r_pr.status_code == 200
    assert len(r_pr.json()) == 3

    r_vd = client.get("/api/v1/environment/combustiveis/vendas/4106902")
    assert r_vd.status_code == 200
    assert len(r_vd.json()) == 2

    r_ps = client.get("/api/v1/environment/combustiveis/postos/Curitiba")
    assert r_ps.status_code == 200
    assert r_ps.json()["qtd_postos_ativos_anp"] > 0

    r_mun = client.get("/api/v1/environment/combustiveis/municipio/4106902")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "PR"

    r_ft = client.get("/api/v1/environment/combustiveis/features/Curitiba")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_matriz_combustiveis_anp"] == "POLO_BIOCOMBUSTIVEL_COMPETITIVO"

    r_rk = client.get("/api/v1/environment/combustiveis/ranking?limit=3&sort_by_biofuel=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["combustiveis", "precos", "Cuiabá", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3
    assert cli_main(["combustiveis", "vendas", "5103403", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["combustiveis", "postos", "Cuiabá", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "MT"
    assert cli_main(["combustiveis", "municipio", "5103403", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5103403"
    assert cli_main(["combustiveis", "features", "Cuiabá", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_competitividade_combustiveis_bio"] >= 75.0
    assert cli_main(["combustiveis", "ranking", "--limit", "3", "--biocombustivel", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_anp_combustiveis_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for ANP Combustiveis."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_anp_combustiveis_precos",
        "bdg_get_anp_combustiveis_vendas",
        "bdg_get_anp_combustiveis_features",
        "bdg_list_top_anp_combustiveis_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_anp_combustiveis_precos", {"termo": "Campinas"}),
        (3, "bdg_get_anp_combustiveis_vendas", {"termo": "3509502"}),
        (4, "bdg_get_anp_combustiveis_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_anp_combustiveis_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_biofuel": True},
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
        assert "anp_combustiveis" in text
