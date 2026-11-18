"""Offline tests and 4-interface parity suite for datasus_extra connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.saude.datasus_extra import DatasusExtraConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_datasus_extra_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = DatasusExtraConnector(settings=settings)
    registered = get_connector("datasus_extra", settings=settings)
    assert isinstance(registered, DatasusExtraConnector)
    assert connector.contract.source_id == "datasus_extra"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "datasus_extra" / "silver"
    gold_dir = tmp_path / "datasus_extra" / "gold"
    assert (silver_dir / "datasus_sia_producao_ambulatorial.parquet").exists()
    assert (silver_dir / "datasus_sinan_agravos_notificacao.parquet").exists()
    assert (silver_dir / "datasus_sisvan_pns_nutricao.parquet").exists()
    assert (silver_dir / "datasus_extra_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_datasus_extra_brasil.parquet").exists()
    assert (tmp_path / "datasus_extra" / "bronze" / "ingestion_meta.json").exists()

    sia = connector.get_datasus_sia_production("Curitiba")
    assert len(sia) == 2
    assert sia[0]["grupo_procedimento_sia"] == "ATENCAO_BASICA_APAC"

    sinan = connector.get_datasus_sinan_notifications("4106902")
    assert len(sinan) == 1
    assert sinan[0]["oportunidade_encerramento_pct"] > 90.0

    sisvan = connector.get_datasus_sisvan_nutrition("Curitiba")
    assert sisvan is not None
    assert sisvan["cobertura_sisvan_atencao_basica_pct"] == 79.2

    summary = connector.get_datasus_extra_summary("4106902")
    assert summary is not None
    assert summary["producao_sia_total_12m"] == 26800000

    features = connector.get_datasus_extra_features("Curitiba")
    assert features is not None
    assert features["tier_capacidade_ambulatorial_sus"] == "POLO_ALTA_RESOLUTIVIDADE_SUS"

    top_sp = connector.list_top_datasus_extra_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_datasus_extra_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK HealthFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_datasus_sia_production("Campinas")) == 2
    assert len(sdk.get_datasus_sinan_notifications("3509502")) == 1

    sisvan = sdk.get_datasus_sisvan_nutrition("3509502")
    assert sisvan is not None
    assert sisvan["acompanhamento_condicoes_cronicas_pct"] >= 80.0

    summary = sdk.get_datasus_extra_summary("Campinas")
    assert summary is not None
    assert summary["sigla_uf"] == "SP"

    features = sdk.get_datasus_extra_features("Campinas")
    assert features is not None
    assert features["score_resolutividade_sus_datasus"] >= 78.0

    top_sia = sdk.list_top_datasus_extra_municipalities(limit=3, sort_by_sia=True)
    assert len(top_sia) == 3
    assert (
        top_sia[0]["producao_sia_total_12m"]
        >= top_sia[1]["producao_sia_total_12m"]
    )


def test_datasus_extra_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg datasus-extra CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_sia = client.get("/api/v1/health/datasus-extra/sia/Fortaleza")
    assert r_sia.status_code == 200
    assert len(r_sia.json()) == 2

    r_sin = client.get("/api/v1/health/datasus-extra/sinan/2304400")
    assert r_sin.status_code == 200
    assert len(r_sin.json()) == 1

    r_sv = client.get("/api/v1/health/datasus-extra/sisvan/Fortaleza")
    assert r_sv.status_code == 200
    assert r_sv.json()["cobertura_sisvan_atencao_basica_pct"] > 70.0

    r_mn = client.get("/api/v1/health/datasus-extra/municipio/2304400")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "CE"

    r_ft = client.get("/api/v1/health/datasus-extra/features/Fortaleza")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_capacidade_ambulatorial_sus"] == "POLO_ALTA_RESOLUTIVIDADE_SUS"

    r_rk = client.get("/api/v1/health/datasus-extra/ranking?limit=3&sort_by_sia=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["datasus-extra", "sia", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["datasus-extra", "sinan", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["datasus-extra", "sisvan", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["datasus-extra", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["datasus-extra", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_resolutividade_sus_datasus"] >= 78.0
    assert cli_main(["datasus-extra", "ranking", "--limit", "3", "--sia", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_datasus_extra_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for DATASUS Extra."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_datasus_sia_production",
        "bdg_get_datasus_sinan_notifications",
        "bdg_get_datasus_extra_features",
        "bdg_list_top_datasus_extra_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_datasus_sia_production", {"termo": "Campinas"}),
        (3, "bdg_get_datasus_sinan_notifications", {"termo": "3509502"}),
        (4, "bdg_get_datasus_extra_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_datasus_extra_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_sia": True},
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
        assert "datasus_extra" in text
