"""Offline tests and 4-interface parity suite for rfb_arrecadacao connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.rfb_arrecadacao import (
    RfbArrecadacaoConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_rfb_arrecadacao_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = RfbArrecadacaoConnector(settings=settings)
    registered = get_connector("rfb_arrecadacao", settings=settings)
    assert isinstance(registered, RfbArrecadacaoConnector)
    assert connector.contract.source_id == "rfb_arrecadacao"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "rfb_arrecadacao" / "silver"
    gold_dir = tmp_path / "rfb_arrecadacao" / "gold"
    assert (silver_dir / "rfb_arrecadacao_tributos_federais.parquet").exists()
    assert (silver_dir / "rfb_divida_ativa_pgfn_estado.parquet").exists()
    assert (silver_dir / "rfb_repasses_fpm_royalties.parquet").exists()
    assert (silver_dir / "rfb_arrecadacao_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_rfb_arrecadacao_brasil.parquet").exists()
    assert (tmp_path / "rfb_arrecadacao" / "bronze" / "ingestion_meta.json").exists()

    trib = connector.get_rfb_tax_collection("Curitiba")
    assert len(trib) == 2
    assert trib[0]["arrecadacao_federal_anual_mi_brl"] > 20000.0

    pgfn = connector.get_pgfn_active_debt("4106902")
    assert len(pgfn) == 1
    assert pgfn[0]["indice_recuperacao_pgfn_pct"] > 14.0

    fpm = connector.get_rfb_fpm_transfers("Curitiba")
    assert fpm is not None
    assert fpm["cota_simples_nacional_munic_mi_brl"] > 2000.0

    summary = connector.get_rfb_arrecadacao_summary("4106902")
    assert summary is not None
    assert summary["contribuintes_pj_ativos_municipio"] == 204600

    features = connector.get_rfb_arrecadacao_features("Curitiba")
    assert features is not None
    assert features["tier_capacidade_fiscal_rfb"] == "POLO_ARRECADACAO_ALTA_CONFORMIDADE"

    top_sp = connector.list_top_rfb_arrecadacao_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_rfb_arrecadacao_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK TreasuryFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_rfb_tax_collection("São Paulo")) == 2
    assert len(sdk.get_pgfn_active_debt("3550308")) == 1

    fpm = sdk.get_rfb_fpm_transfers("3550308")
    assert fpm is not None
    assert fpm["repasses_fpm_anual_mi_brl"] > 1900.0

    summary = sdk.get_rfb_arrecadacao_summary("São Paulo")
    assert summary is not None
    assert summary["sigla_uf"] == "SP"

    features = sdk.get_rfb_arrecadacao_features("São Paulo")
    assert features is not None
    assert features["score_dinamica_arrecadacao_rfb"] >= 78.0

    top_rev = sdk.list_top_rfb_arrecadacao_municipalities(limit=3, sort_by_revenue=True)
    assert len(top_rev) == 3
    assert (
        top_rev[0]["arrecadacao_federal_total_mi_brl"]
        >= top_rev[1]["arrecadacao_federal_total_mi_brl"]
    )


def test_rfb_arrecadacao_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg rfb-arrecadacao CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_tr = client.get("/api/v1/treasury/rfb-arrecadacao/tributos/Brasilia")
    assert r_tr.status_code == 200
    assert len(r_tr.json()) == 2

    r_pg = client.get("/api/v1/treasury/rfb-arrecadacao/pgfn/5300108")
    assert r_pg.status_code == 200
    assert len(r_pg.json()) == 1

    r_fp = client.get("/api/v1/treasury/rfb-arrecadacao/fpm/5300108")
    assert r_fp.status_code == 200
    assert r_fp.json()["cota_simples_nacional_munic_mi_brl"] > 2000.0

    r_mn = client.get("/api/v1/treasury/rfb-arrecadacao/municipio/Brasilia")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "DF"

    r_ft = client.get("/api/v1/treasury/rfb-arrecadacao/features/5300108")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_capacidade_fiscal_rfb"] == "POLO_ARRECADACAO_ALTA_CONFORMIDADE"

    r_rk = client.get("/api/v1/treasury/rfb-arrecadacao/ranking?limit=3&sort_by_revenue=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["rfb-arrecadacao", "tributos", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["rfb-arrecadacao", "pgfn", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["rfb-arrecadacao", "fpm", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["rfb-arrecadacao", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["contribuintes_pj_ativos_municipio"] == 214600
    assert cli_main(["rfb-arrecadacao", "features", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_dinamica_arrecadacao_rfb"] >= 78.0
    assert cli_main(["rfb-arrecadacao", "ranking", "--limit", "3", "--revenue", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_rfb_arrecadacao_mcp_parity(tmp_path: Path) -> None:
    """Validate MCP tool discovery and execution with security boundary wrapping."""
    dispatcher = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))
    listed = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in listed["result"]["tools"]}
    for expected in (
        "bdg_get_rfb_tax_collection",
        "bdg_get_pgfn_active_debt",
        "bdg_get_rfb_arrecadacao_features",
        "bdg_list_top_rfb_arrecadacao_municipalities",
    ):
        assert expected in tool_names

    for req_id, name, arguments in (
        (2, "bdg_get_rfb_tax_collection", {"termo": "Campinas"}),
        (3, "bdg_get_pgfn_active_debt", {"termo": "3509502"}),
        (4, "bdg_get_rfb_arrecadacao_features", {"id_municipio_or_name": "Campinas"}),
        (5, "bdg_list_top_rfb_arrecadacao_municipalities", {"limit": 3, "sort_by_revenue": True}),
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
        assert "rfb_arrecadacao" in text
