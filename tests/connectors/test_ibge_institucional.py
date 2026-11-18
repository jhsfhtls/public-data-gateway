"""Offline tests and 4-interface parity suite for ibge_institucional connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.demografico.ibge_institucional import (
    IbgeInstitucionalConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_ibge_institucional_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = IbgeInstitucionalConnector(settings=settings)
    registered = get_connector("ibge_institucional", settings=settings)
    assert isinstance(registered, IbgeInstitucionalConnector)
    assert connector.contract.source_id == "ibge_institucional"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "ibge_institucional" / "silver"
    gold_dir = tmp_path / "ibge_institucional" / "gold"
    assert (silver_dir / "ibge_munic_governanca_municipal.parquet").exists()
    assert (silver_dir / "ibge_estadic_capacidade_estadual.parquet").exists()
    assert (silver_dir / "ibge_pof_orcamentos_consumo.parquet").exists()
    assert (silver_dir / "ibge_institucional_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_ibge_institucional_brasil.parquet").exists()
    assert (tmp_path / "ibge_institucional" / "bronze" / "ingestion_meta.json").exists()

    munic = connector.get_ibge_munic_governance("Curitiba")
    assert len(munic) == 2
    assert munic[0]["indice_capacidade_institucional_munic"] > 90.0

    estadic = connector.get_ibge_estadic_state_capacity("4106902")
    assert len(estadic) == 1
    assert estadic[0]["cobertura_sistema_protecao_civil_pct"] > 90.0

    pof = connector.get_ibge_pof_budgets("Curitiba")
    assert pof is not None
    assert pof["despesa_mensal_familiar_pof_brl"] > 7000.0

    summary = connector.get_ibge_institucional_summary("4106902")
    assert summary is not None
    assert summary["servidores_estatutarios_municipio"] == 21840

    features = connector.get_ibge_institucional_features("Curitiba")
    assert features is not None
    assert features["tier_governanca_publica_ibge"] == "GOVERNANCA_INSTITUCIONAL_AVANCADA"

    top_sp = connector.list_top_ibge_institucional_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_ibge_institucional_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK SocioeconomicFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_ibge_munic_governance("São Paulo")) == 2
    assert len(sdk.get_ibge_estadic_state_capacity("3550308")) == 1

    pof = sdk.get_ibge_pof_budgets("3550308")
    assert pof is not None
    assert pof["renda_monetaria_media_domiciliar_brl"] > 9000.0

    summary = sdk.get_ibge_institucional_summary("São Paulo")
    assert summary is not None
    assert summary["sigla_uf"] == "SP"

    features = sdk.get_ibge_institucional_features("São Paulo")
    assert features is not None
    assert features["score_capacidade_institucional_ibge"] >= 78.0

    top_pof = sdk.list_top_ibge_institucional_municipalities(limit=3, sort_by_pof=True)
    assert len(top_pof) == 3
    assert (
        top_pof[0]["despesa_familiar_media_pof_brl"]
        >= top_pof[1]["despesa_familiar_media_pof_brl"]
    )


def test_ibge_institucional_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg ibge-institucional CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_mn = client.get("/api/v1/demographics/ibge-institucional/munic/Florianopolis")
    assert r_mn.status_code == 200
    assert len(r_mn.json()) == 2

    r_es = client.get("/api/v1/demographics/ibge-institucional/estadic/4205407")
    assert r_es.status_code == 200
    assert len(r_es.json()) == 1

    r_pf = client.get("/api/v1/demographics/ibge-institucional/pof/4205407")
    assert r_pf.status_code == 200
    assert r_pf.json()["despesa_mensal_familiar_pof_brl"] > 7000.0

    r_mu = client.get("/api/v1/demographics/ibge-institucional/municipio/Florianopolis")
    assert r_mu.status_code == 200
    assert r_mu.json()["sigla_uf"] == "SC"

    r_ft = client.get("/api/v1/demographics/ibge-institucional/features/4205407")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_governanca_publica_ibge"] == "GOVERNANCA_INSTITUCIONAL_AVANCADA"

    r_rk = client.get("/api/v1/demographics/ibge-institucional/ranking?limit=3&sort_by_pof=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["ibge-institucional", "munic", "4205407", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["ibge-institucional", "estadic", "4205407", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["ibge-institucional", "pof", "4205407", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "SC"
    assert cli_main(["ibge-institucional", "municipio", "4205407", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["servidores_estatutarios_municipio"] == 9420
    assert cli_main(["ibge-institucional", "features", "4205407", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_capacidade_institucional_ibge"] >= 78.0
    assert cli_main(["ibge-institucional", "ranking", "--limit", "3", "--pof", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_ibge_institucional_mcp_parity(tmp_path: Path) -> None:
    """Validate MCP tool discovery and execution with security boundary wrapping."""
    dispatcher = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))
    listed = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in listed["result"]["tools"]}
    for expected in (
        "bdg_get_ibge_munic_governance",
        "bdg_get_ibge_pof_budgets",
        "bdg_get_ibge_institucional_features",
        "bdg_list_top_ibge_institucional_municipalities",
    ):
        assert expected in tool_names

    for req_id, name, arguments in (
        (2, "bdg_get_ibge_munic_governance", {"termo": "Campinas"}),
        (3, "bdg_get_ibge_pof_budgets", {"id_municipio_or_name": "3509502"}),
        (4, "bdg_get_ibge_institucional_features", {"id_municipio_or_name": "Campinas"}),
        (5, "bdg_list_top_ibge_institucional_municipalities", {"limit": 3, "sort_by_pof": True}),
    ):
        resp = dispatcher.handle_request(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        assert resp["result"]["isError"] is False
        text = resp["result"]["content"][0]["text"]
        assert "ibge_institucional" in text
