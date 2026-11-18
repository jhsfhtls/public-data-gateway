"""Offline tests and 4-interface parity suite for senatran_frota connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.geografico.senatran_frota import (
    SenatranFrotaConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_senatran_frota_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = SenatranFrotaConnector(settings=settings)
    registered = get_connector("senatran_frota", settings=settings)
    assert isinstance(registered, SenatranFrotaConnector)
    assert connector.contract.source_id == "senatran_frota"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "senatran_frota" / "silver"
    gold_dir = tmp_path / "senatran_frota" / "gold"
    assert (silver_dir / "senatran_frota_tipo_municipio.parquet").exists()
    assert (silver_dir / "senatran_frota_combustivel_ev.parquet").exists()
    assert (silver_dir / "senatran_idade_renovacao_frota.parquet").exists()
    assert (silver_dir / "senatran_frota_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_mobilidade_frota_senatran.parquet").exists()
    assert (tmp_path / "senatran_frota" / "bronze" / "ingestion_meta.json").exists()

    tipos = connector.get_fleet_by_type("Curitiba")
    assert len(tipos) == 3
    assert all(t["sigla_uf"] == "PR" for t in tipos)

    ev = connector.get_fleet_propulsion_ev("4106902")
    assert len(ev) == 2
    assert any(p["matriz_propulsao"] == "ELETRICO_BEV_HIBRIDO_PHEV" for p in ev)

    idade = connector.get_fleet_age_renewal("Curitiba")
    assert idade is not None
    assert idade["idade_media_frota_anos"] > 0

    summary = connector.get_fleet_municipal_summary("4106902")
    assert summary is not None
    assert summary["frota_total_veiculos"] > 1000000

    features = connector.get_fleet_features("Curitiba")
    assert features is not None
    assert features["tier_mobilidade_frota_senatran"] == "POLO_MOBILIDADE_FROTA_MODERNA"

    top_sp = connector.list_top_fleet_municipalities(limit=3, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_senatran_frota_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK GeoFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_fleet_by_type("Campinas")) == 3
    assert len(sdk.get_fleet_propulsion_ev("3509502")) == 2

    idade = sdk.get_fleet_age_renewal("3509502")
    assert idade is not None
    assert idade["taxa_motorizacao_100hab"] >= 70.0

    summary = sdk.get_fleet_municipal_summary("Campinas")
    assert summary is not None
    assert summary["qtd_eletrificados_bevs_hevs"] > 10000

    features = sdk.get_fleet_features("Campinas")
    assert features is not None
    assert features["score_modernizacao_frota_mobilidade"] >= 75.0

    top = sdk.list_top_fleet_municipalities(limit=3, sort_by_ev=True)
    assert len(top) == 3
    assert top[0]["qtd_eletrificados_bevs_hevs"] >= top[1]["qtd_eletrificados_bevs_hevs"]


def test_senatran_frota_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg frota CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_tp = client.get("/api/v1/geo/frota/tipos/Brasília")
    assert r_tp.status_code == 200
    assert len(r_tp.json()) == 3

    r_pr = client.get("/api/v1/geo/frota/propulsao/5300108")
    assert r_pr.status_code == 200
    assert len(r_pr.json()) == 2

    r_id = client.get("/api/v1/geo/frota/idade/Brasília")
    assert r_id.status_code == 200
    assert r_id.json()["idade_media_frota_anos"] > 0

    r_mun = client.get("/api/v1/geo/frota/municipio/5300108")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "DF"

    r_ft = client.get("/api/v1/geo/frota/features/Brasília")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_mobilidade_frota_senatran"] == "POLO_MOBILIDADE_FROTA_MODERNA"

    r_rk = client.get("/api/v1/geo/frota/ranking?limit=3&sort_by_ev=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["frota", "tipos", "São Paulo", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3
    assert cli_main(["frota", "propulsao", "3550308", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["frota", "idade", "São Paulo", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "SP"
    assert cli_main(["frota", "municipio", "3550308", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "3550308"
    assert cli_main(["frota", "features", "São Paulo", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_modernizacao_frota_mobilidade"] >= 75.0
    assert cli_main(["frota", "ranking", "--limit", "3", "--ev", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_senatran_frota_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for SENATRAN Frota."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_senatran_frota_tipos",
        "bdg_get_senatran_frota_propulsao_ev",
        "bdg_get_senatran_frota_features",
        "bdg_list_top_senatran_frota_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_senatran_frota_tipos", {"termo": "São Paulo"}),
        (3, "bdg_get_senatran_frota_propulsao_ev", {"termo": "3550308"}),
        (4, "bdg_get_senatran_frota_features", {"id_municipio_or_name": "São Paulo"}),
        (
            5,
            "bdg_list_top_senatran_frota_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_ev": True},
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
        assert "senatran_frota" in text
