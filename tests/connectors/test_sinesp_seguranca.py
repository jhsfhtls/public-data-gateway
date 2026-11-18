"""Offline tests and 4-interface parity suite for sinesp_seguranca connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.sinesp_seguranca import (
    SinespSegurancaConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_sinesp_seguranca_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = SinespSegurancaConnector(settings=settings)
    registered = get_connector("sinesp_seguranca", settings=settings)
    assert isinstance(registered, SinespSegurancaConnector)
    assert connector.contract.source_id == "sinesp_seguranca"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "sinesp_seguranca" / "silver"
    gold_dir = tmp_path / "sinesp_seguranca" / "gold"
    assert (silver_dir / "sinesp_ocorrencias_municipais.parquet").exists()
    assert (silver_dir / "sinesp_vitimizacao_mvi_uf.parquet").exists()
    assert (silver_dir / "sinesp_capacidade_seguranca.parquet").exists()
    assert (silver_dir / "sinesp_seguranca_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_risco_seguranca_sinesp.parquet").exists()
    assert (tmp_path / "sinesp_seguranca" / "bronze" / "ingestion_meta.json").exists()

    crimes = connector.get_sinesp_crimes("Campinas")
    assert len(crimes) == 3
    assert all(c["sigla_uf"] == "SP" for c in crimes)

    mvi = connector.get_sinesp_mvi_victims("3509502")
    assert len(mvi) == 1
    assert mvi[0]["taxa_mvi_por_100k_hab"] > 0

    cap = connector.get_sinesp_security_capacity("Campinas")
    assert cap is not None
    assert cap["qtd_delegacias_unidades"] > 0

    summary = connector.get_sinesp_municipal_summary("3509502")
    assert summary is not None
    assert summary["investimento_seguranca_per_capita_brl"] > 0

    features = connector.get_sinesp_features("Campinas")
    assert features is not None
    assert features["tier_risco_seguranca_territorial"] == "ALTA_SEGURANCA_TERRITORIAL"

    top_sp = connector.list_top_safest_sinesp_municipalities(limit=3, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_sinesp_seguranca_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK ComplianceFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_sinesp_crimes("Brasília")) == 3
    assert len(sdk.get_sinesp_mvi_victims("5300108")) == 1

    cap = sdk.get_sinesp_security_capacity("5300108")
    assert cap is not None
    assert cap["efetivo_seguranca_100k_hab"] >= 300.0

    summary = sdk.get_sinesp_municipal_summary("Brasília")
    assert summary is not None
    assert summary["sigla_uf"] == "DF"

    features = sdk.get_sinesp_features("Brasília")
    assert features is not None
    assert features["score_seguranca_publica_sinesp"] >= 75.0

    top = sdk.list_top_safest_sinesp_municipalities(limit=3, sort_by_cargo_theft=True)
    assert len(top) == 3
    assert top[0]["taxa_roubo_carga_100k_hab"] >= top[1]["taxa_roubo_carga_100k_hab"]


def test_sinesp_seguranca_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg seguranca CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_oc = client.get("/api/v1/compliance/seguranca/ocorrencias/Curitiba")
    assert r_oc.status_code == 200
    assert len(r_oc.json()) == 3

    r_mv = client.get("/api/v1/compliance/seguranca/vitimas/4106902")
    assert r_mv.status_code == 200
    assert len(r_mv.json()) == 1

    r_cp = client.get("/api/v1/compliance/seguranca/capacidade/Curitiba")
    assert r_cp.status_code == 200
    assert r_cp.json()["possui_guarda_municipal_ativa"] is True

    r_mun = client.get("/api/v1/compliance/seguranca/municipio/4106902")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "PR"

    r_ft = client.get("/api/v1/compliance/seguranca/features/Curitiba")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_risco_seguranca_territorial"] == "ALTA_SEGURANCA_TERRITORIAL"

    r_rk = client.get("/api/v1/compliance/seguranca/ranking?limit=3&sort_by_cargo_theft=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["seguranca", "ocorrencias", "São Paulo", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3
    assert cli_main(["seguranca", "vitimas", "3550308", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["seguranca", "capacidade", "São Paulo", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "SP"
    assert cli_main(["seguranca", "municipio", "3550308", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "3550308"
    assert cli_main(["seguranca", "features", "São Paulo", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_seguranca_publica_sinesp"] >= 75.0
    assert cli_main(["seguranca", "ranking", "--limit", "3", "--roubo-carga", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_sinesp_seguranca_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for SINESP Seguranca."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_sinesp_ocorrencias",
        "bdg_get_sinesp_vitimas_mvi",
        "bdg_get_sinesp_seguranca_features",
        "bdg_list_top_sinesp_seguranca_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_sinesp_ocorrencias", {"termo": "São Paulo"}),
        (3, "bdg_get_sinesp_vitimas_mvi", {"termo": "3550308"}),
        (4, "bdg_get_sinesp_seguranca_features", {"id_municipio_or_name": "São Paulo"}),
        (
            5,
            "bdg_list_top_sinesp_seguranca_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_cargo_theft": True},
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
        assert "sinesp_seguranca" in text
