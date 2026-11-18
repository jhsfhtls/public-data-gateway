"""Offline tests and 4-interface parity suite for cgu_servidores_federais connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
    CguServidoresFederaisConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_cgu_servidores_federais_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = CguServidoresFederaisConnector(settings=settings)
    registered = get_connector("cgu_servidores_federais", settings=settings)
    assert isinstance(registered, CguServidoresFederaisConnector)
    assert connector.contract.source_id == "cgu_servidores_federais"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "cgu_servidores_federais" / "silver"
    gold_dir = tmp_path / "cgu_servidores_federais" / "gold"
    assert (silver_dir / "siape_servidores_ativos_municipio.parquet").exists()
    assert (silver_dir / "siape_remuneracao_folha_orgao.parquet").exists()
    assert (silver_dir / "cgu_acumulo_cargos_conflitos.parquet").exists()
    assert (silver_dir / "siape_servidores_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_servidores_siape_cgu.parquet").exists()
    assert (tmp_path / "cgu_servidores_federais" / "bronze" / "ingestion_meta.json").exists()

    lot = connector.get_siape_active_staff("Curitiba")
    assert len(lot) == 2
    assert lot[0]["sigla_uf"] == "PR"

    folha = connector.get_siape_payroll("4106902")
    assert len(folha) == 1
    assert folha[0]["remuneracao_media_bruta_brl"] > 10000.0

    comp = connector.get_cgu_staff_compliance("Curitiba")
    assert comp is not None
    assert comp["indice_integridade_siape_pct"] > 90.0

    summary = connector.get_siape_municipal_summary("4106902")
    assert summary is not None
    assert summary["total_servidores_federais_siape"] == 18400

    features = connector.get_siape_features("Curitiba")
    assert features is not None
    assert features["tier_governanca_pessoal_siape"] == "ALTA_GOVERNANCA_FUNCIONAL_SIAPE"

    top_sp = connector.list_top_siape_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_cgu_servidores_federais_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK ComplianceFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_siape_active_staff("Campinas")) == 2
    assert len(sdk.get_siape_payroll("3509502")) == 1

    comp = sdk.get_cgu_staff_compliance("3509502")
    assert comp is not None
    assert comp["servidores_dedicacao_exclusiva_pct"] >= 90.0

    summary = sdk.get_siape_municipal_summary("Campinas")
    assert summary is not None
    assert summary["folha_mensal_bruta_siape_mil_brl"] > 0

    features = sdk.get_siape_features("Campinas")
    assert features is not None
    assert features["score_integridade_funcional_siape"] >= 75.0

    top_pay = sdk.list_top_siape_municipalities(limit=3, sort_by_payroll=True)
    assert len(top_pay) == 3
    assert top_pay[0]["folha_mensal_bruta_siape_mil_brl"] >= top_pay[1]["folha_mensal_bruta_siape_mil_brl"]


def test_cgu_servidores_federais_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg servidores-siape CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_lot = client.get("/api/v1/compliance/servidores-siape/lotacao/Goiânia")
    assert r_lot.status_code == 200
    assert len(r_lot.json()) == 2

    r_fol = client.get("/api/v1/compliance/servidores-siape/folha/5208707")
    assert r_fol.status_code == 200
    assert len(r_fol.json()) == 1

    r_ac = client.get("/api/v1/compliance/servidores-siape/acumulo/Goiânia")
    assert r_ac.status_code == 200
    assert r_ac.json()["indice_integridade_siape_pct"] > 85.0

    r_mn = client.get("/api/v1/compliance/servidores-siape/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/compliance/servidores-siape/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_governanca_pessoal_siape"] == "ALTA_GOVERNANCA_FUNCIONAL_SIAPE"

    r_rk = client.get("/api/v1/compliance/servidores-siape/ranking?limit=3&sort_by_payroll=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["servidores-siape", "lotacao", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["servidores-siape", "folha", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["servidores-siape", "acumulo", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["servidores-siape", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["servidores-siape", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_integridade_funcional_siape"] >= 75.0
    assert cli_main(["servidores-siape", "ranking", "--limit", "3", "--folha", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_cgu_servidores_federais_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for CGU/SIAPE."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_cgu_servidores_lotacao",
        "bdg_get_cgu_servidores_folha",
        "bdg_get_cgu_servidores_features",
        "bdg_list_top_cgu_servidores_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_cgu_servidores_lotacao", {"termo": "Campinas"}),
        (3, "bdg_get_cgu_servidores_folha", {"termo": "3509502"}),
        (4, "bdg_get_cgu_servidores_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_cgu_servidores_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_payroll": True},
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
        assert "cgu_servidores_federais" in text
