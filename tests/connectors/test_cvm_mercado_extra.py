"""Offline tests and 4-interface parity suite for cvm_mercado_extra connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
    CvmMercadoExtraConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_cvm_mercado_extra_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = CvmMercadoExtraConnector(settings=settings)
    registered = get_connector("cvm_mercado_extra", settings=settings)
    assert isinstance(registered, CvmMercadoExtraConnector)
    assert connector.contract.source_id == "cvm_mercado_extra"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "cvm_mercado_extra" / "silver"
    gold_dir = tmp_path / "cvm_mercado_extra" / "gold"
    assert (silver_dir / "cvm_ofertas_distrib_rcvm160.parquet").exists()
    assert (silver_dir / "cvm_administradores_fii_carteira.parquet").exists()
    assert (silver_dir / "cvm_agentes_autonomos_assessoria.parquet").exists()
    assert (silver_dir / "cvm_mercado_extra_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_cvm_mercado_extra_brasil.parquet").exists()
    assert (tmp_path / "cvm_mercado_extra" / "bronze" / "ingestion_meta.json").exists()

    ofertas = connector.get_rcvm160_offerings("Curitiba")
    assert len(ofertas) == 2
    assert ofertas[0]["sigla_uf"] == "PR"

    adms = connector.get_fii_administrators("4106902")
    assert len(adms) == 1
    assert adms[0]["patrimonio_administrado_mi_brl"] > 100000.0

    aais = connector.get_investment_advisors("Curitiba")
    assert aais is not None
    assert aais["assessores_investimento_ativos"] > 1000

    summary = connector.get_cvm_extra_summary("4106902")
    assert summary is not None
    assert summary["volume_captado_rcvm160_mil_brl"] > 10_000_000.0

    features = connector.get_cvm_extra_features("Curitiba")
    assert features is not None
    assert features["tier_ecossistema_mercado_capitais"] == "HUB_MERCADO_CAPITAIS_CVM"

    top_sp = connector.list_top_cvm_extra_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_cvm_mercado_extra_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK FinanceFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_rcvm160_offerings("Campinas")) == 2
    assert len(sdk.get_fii_administrators("3509502")) == 1

    aais = sdk.get_investment_advisors("3509502")
    assert aais is not None
    assert aais["sociedades_aai_registradas"] > 50

    summary = sdk.get_cvm_extra_summary("Campinas")
    assert summary is not None
    assert summary["captacao_per_capita_brl"] > 0

    features = sdk.get_cvm_extra_features("Campinas")
    assert features is not None
    assert features["score_sofisticacao_mercado_cvm"] >= 76.0

    top_vol = sdk.list_top_cvm_extra_municipalities(limit=3, sort_by_volume=True)
    assert len(top_vol) == 3
    assert top_vol[0]["volume_captado_rcvm160_mil_brl"] >= top_vol[1]["volume_captado_rcvm160_mil_brl"]


def test_cvm_mercado_extra_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg cvm-extra CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_of = client.get("/api/v1/finance/cvm-extra/ofertas/Goiânia")
    assert r_of.status_code == 200
    assert len(r_of.json()) == 2

    r_adm = client.get("/api/v1/finance/cvm-extra/administradores/5208707")
    assert r_adm.status_code == 200
    assert len(r_adm.json()) == 1

    r_ag = client.get("/api/v1/finance/cvm-extra/agentes/Goiânia")
    assert r_ag.status_code == 200
    assert r_ag.json()["assessores_investimento_ativos"] > 500

    r_mn = client.get("/api/v1/finance/cvm-extra/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/finance/cvm-extra/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_ecossistema_mercado_capitais"] == "HUB_MERCADO_CAPITAIS_CVM"

    r_rk = client.get("/api/v1/finance/cvm-extra/ranking?limit=3&sort_by_volume=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["cvm-extra", "ofertas", "Fortaleza", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["cvm-extra", "administradores", "2304400", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["cvm-extra", "agentes", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "CE"
    assert cli_main(["cvm-extra", "municipio", "2304400", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "2304400"
    assert cli_main(["cvm-extra", "features", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_sofisticacao_mercado_cvm"] >= 76.0
    assert cli_main(["cvm-extra", "ranking", "--limit", "3", "--volume", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_cvm_mercado_extra_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for CVM Mercado Extra."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_cvm_ofertas_rcvm160",
        "bdg_get_cvm_administradores",
        "bdg_get_cvm_extra_features",
        "bdg_list_top_cvm_extra_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_cvm_ofertas_rcvm160", {"termo": "Campinas"}),
        (3, "bdg_get_cvm_administradores", {"termo": "3509502"}),
        (4, "bdg_get_cvm_extra_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_cvm_extra_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_volume": True},
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
        assert "cvm_mercado_extra" in text
