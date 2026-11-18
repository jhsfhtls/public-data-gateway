"""Offline tests and 4-interface parity suite for inpi_propriedade_intelectual connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
    InpiPropriedadeIntelectualConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_inpi_pi_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = InpiPropriedadeIntelectualConnector(settings=settings)
    registered = get_connector("inpi_propriedade_intelectual", settings=settings)
    assert isinstance(registered, InpiPropriedadeIntelectualConnector)
    assert connector.contract.source_id == "inpi_propriedade_intelectual"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "inpi_propriedade_intelectual" / "silver"
    gold_dir = tmp_path / "inpi_propriedade_intelectual" / "gold"
    assert (silver_dir / "inpi_patentes_depositos_municipio.parquet").exists()
    assert (silver_dir / "inpi_marcas_desenhos_industriais.parquet").exists()
    assert (silver_dir / "inpi_software_transferencia_tecnologia.parquet").exists()
    assert (silver_dir / "inpi_pi_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_inpi_pi_brasil.parquet").exists()
    assert (tmp_path / "inpi_propriedade_intelectual" / "bronze" / "ingestion_meta.json").exists()

    pat = connector.get_inpi_patentes("Campinas")
    assert len(pat) == 2
    assert pat[0]["natureza_ativo_pi"] == "PATENTE_INVECAO"

    mrc = connector.get_inpi_marcas("3509502")
    assert len(mrc) == 1
    assert mrc[0]["registros_marcas_ativos"] > 5000

    sw = connector.get_inpi_software_transferencia("Florianópolis")
    assert sw is not None
    assert sw["registros_programa_computador"] == 385

    summary = connector.get_inpi_municipal_summary("4205407")
    assert summary is not None
    assert summary["patentes_por_100k_hab"] > 60.0

    features = connector.get_inpi_features("São Paulo")
    assert features is not None
    assert features["tier_inovacao_tecnologica_inpi"] == "POLO_DEEP_TECH_INOVACAO_INPI"

    top_sp = connector.list_top_inpi_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 3
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_inpi_pi_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK CompanyFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_inpi_patentes("São José dos Campos")) == 2
    assert len(sdk.get_inpi_marcas("3549904")) == 1

    sw = sdk.get_inpi_software_transferencia("3549904")
    assert sw is not None
    assert sw["contratos_transferencia_tecnologia"] >= 100

    summary = sdk.get_inpi_municipal_summary("São José dos Campos")
    assert summary is not None
    assert summary["sigla_uf"] == "SP"

    features = sdk.get_inpi_features("São José dos Campos")
    assert features is not None
    assert features["score_inovacao_propriedade_intelectual"] >= 78.0

    top_pat = sdk.list_top_inpi_municipalities(limit=3, sort_by_patentes=True)
    assert len(top_pat) == 3
    assert (
        top_pat[0]["patentes_depositadas_12m"]
        >= top_pat[1]["patentes_depositadas_12m"]
    )


def test_inpi_pi_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg inpi-pi CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_pat = client.get("/api/v1/company/inpi/patentes/Curitiba")
    assert r_pat.status_code == 200
    assert len(r_pat.json()) == 2

    r_mrc = client.get("/api/v1/company/inpi/marcas/4106902")
    assert r_mrc.status_code == 200
    assert len(r_mrc.json()) == 1

    r_sw = client.get("/api/v1/company/inpi/software/Curitiba")
    assert r_sw.status_code == 200
    assert r_sw.json()["registros_programa_computador"] > 0

    r_mn = client.get("/api/v1/company/inpi/municipio/4106902")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "PR"

    r_ft = client.get("/api/v1/company/inpi/features/Curitiba")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_inovacao_tecnologica_inpi"] == "POLO_DEEP_TECH_INOVACAO_INPI"

    r_rk = client.get("/api/v1/company/inpi/ranking?limit=3&sort_by_patentes=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["inpi-pi", "patentes", "Recife", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["inpi-pi", "marcas", "2611606", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["inpi-pi", "software", "Recife", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "PE"
    assert cli_main(["inpi-pi", "municipio", "2611606", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "2611606"
    assert cli_main(["inpi-pi", "features", "Recife", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_inovacao_propriedade_intelectual"] >= 78.0
    assert cli_main(["inpi-pi", "ranking", "--limit", "3", "--patentes", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_inpi_pi_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for INPI Propriedade Intelectual."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_inpi_patentes",
        "bdg_get_inpi_marcas",
        "bdg_get_inpi_features",
        "bdg_list_top_inpi_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_inpi_patentes", {"termo": "Campinas"}),
        (3, "bdg_get_inpi_marcas", {"termo": "3509502"}),
        (4, "bdg_get_inpi_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_inpi_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_patentes": True},
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
        assert "inpi_propriedade_intelectual" in text
