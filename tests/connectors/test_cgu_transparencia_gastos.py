"""Offline tests and 4-interface parity suite for cgu_transparencia_gastos connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
    CguTransparenciaGastosConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_cgu_transparencia_gastos_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = CguTransparenciaGastosConnector(settings=settings)
    registered = get_connector("cgu_transparencia_gastos", settings=settings)
    assert isinstance(registered, CguTransparenciaGastosConnector)
    assert connector.contract.source_id == "cgu_transparencia_gastos"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "cgu_transparencia_gastos" / "silver"
    gold_dir = tmp_path / "cgu_transparencia_gastos" / "gold"
    assert (silver_dir / "cgu_gastos_diretos_orgaos.parquet").exists()
    assert (silver_dir / "cgu_transferencias_constitucionais_fpm.parquet").exists()
    assert (silver_dir / "cgu_emendas_convenios_municipio.parquet").exists()
    assert (silver_dir / "cgu_transparencia_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_transparencia_gastos_cgu.parquet").exists()
    assert (tmp_path / "cgu_transparencia_gastos" / "bronze" / "ingestion_meta.json").exists()

    desp = connector.get_cgu_direct_expenses("Curitiba")
    assert len(desp) == 2
    assert desp[0]["sigla_uf"] == "PR"

    trf = connector.get_cgu_constitutional_transfers("4106902")
    assert len(trf) == 1
    assert trf[0]["repasse_fpm_fundeb_mil_brl"] > 0

    em = connector.get_cgu_amendments_covenants("Curitiba")
    assert em is not None
    assert em["qtd_convenios_ativos"] > 0

    summary = connector.get_cgu_expenses_summary("4106902")
    assert summary is not None
    assert summary["total_gastos_federais_mil_brl"] == 1840000.0

    features = connector.get_cgu_expenses_features("Curitiba")
    assert features is not None
    assert features["tier_transparencia_gastos_cgu"] == "EXCELENCIA_TRANSPARENCIA_EXECUCAO"

    top_sp = connector.list_top_cgu_transparency_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_cgu_transparencia_gastos_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK ComplianceFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_cgu_direct_expenses("Campinas")) == 2
    assert len(sdk.get_cgu_constitutional_transfers("3509502")) == 1

    em = sdk.get_cgu_amendments_covenants("3509502")
    assert em is not None
    assert em["adimplencia_prestacao_contas_pct"] >= 90.0

    summary = sdk.get_cgu_expenses_summary("Campinas")
    assert summary is not None
    assert summary["taxa_execucao_orcamentaria_pct"] >= 85.0

    features = sdk.get_cgu_expenses_features("Campinas")
    assert features is not None
    assert features["score_integridade_execucao_cgu"] >= 75.0

    top_spnd = sdk.list_top_cgu_transparency_municipalities(limit=3, sort_by_spending=True)
    assert len(top_spnd) == 3
    assert top_spnd[0]["total_gastos_federais_mil_brl"] >= top_spnd[1]["total_gastos_federais_mil_brl"]


def test_cgu_transparencia_gastos_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg transparencia CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_dp = client.get("/api/v1/compliance/transparencia-gastos/despesas/Goiânia")
    assert r_dp.status_code == 200
    assert len(r_dp.json()) == 2

    r_tf = client.get("/api/v1/compliance/transparencia-gastos/transferencias/5208707")
    assert r_tf.status_code == 200
    assert len(r_tf.json()) == 1

    r_em = client.get("/api/v1/compliance/transparencia-gastos/emendas/Goiânia")
    assert r_em.status_code == 200
    assert r_em.json()["emendas_parlamentares_pagas_mil_brl"] > 0

    r_mn = client.get("/api/v1/compliance/transparencia-gastos/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/compliance/transparencia-gastos/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_transparencia_gastos_cgu"] == "EXCELENCIA_TRANSPARENCIA_EXECUCAO"

    r_rk = client.get("/api/v1/compliance/transparencia-gastos/ranking?limit=3&sort_by_spending=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["transparencia", "despesas", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["transparencia", "transferencias", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["transparencia", "emendas", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["transparencia", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["transparencia", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_integridade_execucao_cgu"] >= 75.0
    assert cli_main(["transparencia", "ranking", "--limit", "3", "--gasto", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_cgu_transparencia_gastos_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for CGU Transparência."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_cgu_gastos_diretos",
        "bdg_get_cgu_transferencias_fpm",
        "bdg_get_cgu_transparencia_features",
        "bdg_list_top_cgu_transparencia_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_cgu_gastos_diretos", {"termo": "Campinas"}),
        (3, "bdg_get_cgu_transferencias_fpm", {"termo": "3509502"}),
        (4, "bdg_get_cgu_transparencia_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_cgu_transparencia_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_spending": True},
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
        assert "cgu_transparencia_gastos" in text
