"""Offline tests and 4-interface parity suite for mdr_snis connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_mdr_snis_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = MdrSnisConnector(settings=settings)
    registered = get_connector("mdr_snis", settings=settings)
    assert isinstance(registered, MdrSnisConnector)
    assert connector.contract.source_id == "mdr_snis"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "mdr_snis" / "silver"
    gold_dir = tmp_path / "mdr_snis" / "gold"
    assert (silver_dir / "snis_agua_esgoto_municipio.parquet").exists()
    assert (silver_dir / "snis_residuos_solidos_municipio.parquet").exists()
    assert (silver_dir / "snis_investimentos_tarifas.parquet").exists()
    assert (silver_dir / "snis_saneamento_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_saneamento_snis.parquet").exists()
    assert (tmp_path / "mdr_snis" / "bronze" / "ingestion_meta.json").exists()

    ae = connector.get_snis_water_sewage("Curitiba")
    assert len(ae) == 1
    assert ae[0]["prestador_servico"] == "SANEPAR"

    rs = connector.get_snis_solid_waste("4106902")
    assert len(rs) == 1
    assert rs[0]["coleta_seletiva_ativa"] is True

    fin = connector.get_snis_financial_investments("Curitiba")
    assert fin is not None
    assert fin["investimento_total_milhoes_brl"] > 0

    summary = connector.get_snis_municipal_summary("4106902")
    assert summary is not None
    assert summary["atendimento_agua_total_pct"] == 100.0

    features = connector.get_snis_features("Curitiba")
    assert features is not None
    assert features["tier_marco_saneamento_2033"] == "UNIVERSALIZADO_META_2033"

    top_sp = connector.list_top_snis_municipalities(limit=3, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_mdr_snis_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK EnvironmentFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_snis_water_sewage("Campinas")) == 1
    assert len(sdk.get_snis_solid_waste("3509502")) == 1

    fin = sdk.get_snis_financial_investments("3509502")
    assert fin is not None
    assert fin["indice_suficiencia_caixa_pct"] >= 100.0

    summary = sdk.get_snis_municipal_summary("Campinas")
    assert summary is not None
    assert summary["prestador_servico"] == "SANASA"

    features = sdk.get_snis_features("Campinas")
    assert features is not None
    assert features["score_universalizacao_saneamento_snis"] >= 75.0

    top = sdk.list_top_snis_municipalities(limit=3, sort_by_sewage=True)
    assert len(top) == 3
    assert top[0]["atendimento_esgoto_total_pct"] >= top[1]["atendimento_esgoto_total_pct"]


def test_mdr_snis_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg saneamento CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_ae = client.get("/api/v1/environment/saneamento/agua-esgoto/Goiânia")
    assert r_ae.status_code == 200
    assert len(r_ae.json()) == 1

    r_rs = client.get("/api/v1/environment/saneamento/residuos/5208707")
    assert r_rs.status_code == 200
    assert len(r_rs.json()) == 1

    r_fin = client.get("/api/v1/environment/saneamento/financeiro/Goiânia")
    assert r_fin.status_code == 200
    assert r_fin.json()["investimento_total_milhoes_brl"] > 0

    r_mun = client.get("/api/v1/environment/saneamento/municipio/5208707")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/environment/saneamento/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_marco_saneamento_2033"] == "UNIVERSALIZADO_META_2033"

    r_rk = client.get("/api/v1/environment/saneamento/ranking?limit=3&sort_by_sewage=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["saneamento", "agua-esgoto", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["saneamento", "residuos", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["saneamento", "financeiro", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["saneamento", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["saneamento", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_universalizacao_saneamento_snis"] >= 75.0
    assert cli_main(["saneamento", "ranking", "--limit", "3", "--esgoto", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_mdr_snis_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for MDR SNIS."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_mdr_snis_agua_esgoto",
        "bdg_get_mdr_snis_residuos",
        "bdg_get_mdr_snis_features",
        "bdg_list_top_mdr_snis_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_mdr_snis_agua_esgoto", {"termo": "Campinas"}),
        (3, "bdg_get_mdr_snis_residuos", {"termo": "3509502"}),
        (4, "bdg_get_mdr_snis_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_mdr_snis_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_sewage": True},
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
        assert "mdr_snis" in text
