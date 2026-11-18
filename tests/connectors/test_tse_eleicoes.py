"""Offline tests and 4-interface parity suite for tse_eleicoes connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.tse_eleicoes import TseEleicoesConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_tse_eleicoes_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = TseEleicoesConnector(settings=settings)
    registered = get_connector("tse_eleicoes", settings=settings)
    assert isinstance(registered, TseEleicoesConnector)
    assert connector.contract.source_id == "tse_eleicoes"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "tse_eleicoes" / "silver"
    gold_dir = tmp_path / "tse_eleicoes" / "gold"
    assert (silver_dir / "tse_candidaturas_partidos_municipio.parquet").exists()
    assert (silver_dir / "tse_votacao_comparecimento_secao.parquet").exists()
    assert (silver_dir / "tse_prestacao_contas_campanha.parquet").exists()
    assert (silver_dir / "tse_eleicoes_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_eleicoes_tse_brasil.parquet").exists()
    assert (tmp_path / "tse_eleicoes" / "bronze" / "ingestion_meta.json").exists()

    cand = connector.get_candidacies("Curitiba")
    assert len(cand) == 2
    assert cand[0]["sigla_uf"] == "PR"

    vot = connector.get_turnout_stats("4106902")
    assert len(vot) == 1
    assert vot[0]["comparecimento_pct"] > 70.0

    contas = connector.get_campaign_finance("Curitiba")
    assert contas is not None
    assert contas["contas_aprovadas_pct"] >= 90.0

    summary = connector.get_electoral_summary("4106902")
    assert summary is not None
    assert summary["eleitorado_apto_total"] > 1_000_000

    features = connector.get_electoral_features("Curitiba")
    assert features is not None
    assert features["tier_maturidade_eleitoral_tse"] == "ALTA_MATURIDADE_ELEITORAL"

    top_sp = connector.list_top_electoral_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_tse_eleicoes_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK ComplianceFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_tse_candidacies("Campinas")) == 2
    assert len(sdk.get_tse_turnout("3509502")) == 1

    contas = sdk.get_tse_campaign_finance("3509502")
    assert contas is not None
    assert contas["doadores_cpf_cnpj_distintos"] > 1000

    summary = sdk.get_tse_municipal_summary("Campinas")
    assert summary is not None
    assert summary["custo_por_eleitor_brl"] > 0

    features = sdk.get_tse_features("Campinas")
    assert features is not None
    assert features["score_integridade_eleitoral_tse"] >= 78.0

    top_elec = sdk.list_top_tse_municipalities(limit=3, sort_by_electorate=True)
    assert len(top_elec) == 3
    assert top_elec[0]["eleitorado_apto_total"] >= top_elec[1]["eleitorado_apto_total"]


def test_tse_eleicoes_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg eleicoes-tse CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_cand = client.get("/api/v1/compliance/eleicoes-tse/candidaturas/Goiânia")
    assert r_cand.status_code == 200
    assert len(r_cand.json()) == 2

    r_vot = client.get("/api/v1/compliance/eleicoes-tse/votacao/5208707")
    assert r_vot.status_code == 200
    assert len(r_vot.json()) == 1

    r_ct = client.get("/api/v1/compliance/eleicoes-tse/contas/Goiânia")
    assert r_ct.status_code == 200
    assert r_ct.json()["contas_aprovadas_pct"] > 80.0

    r_mn = client.get("/api/v1/compliance/eleicoes-tse/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/compliance/eleicoes-tse/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_maturidade_eleitoral_tse"] == "ALTA_MATURIDADE_ELEITORAL"

    r_rk = client.get("/api/v1/compliance/eleicoes-tse/ranking?limit=3&sort_by_electorate=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["eleicoes-tse", "candidaturas", "Fortaleza", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["eleicoes-tse", "votacao", "2304400", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["eleicoes-tse", "contas", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "CE"
    assert cli_main(["eleicoes-tse", "municipio", "2304400", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "2304400"
    assert cli_main(["eleicoes-tse", "features", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_integridade_eleitoral_tse"] >= 78.0
    assert cli_main(["eleicoes-tse", "ranking", "--limit", "3", "--eleitorado", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_tse_eleicoes_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for TSE Eleições."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_tse_candidaturas",
        "bdg_get_tse_votacao",
        "bdg_get_tse_features",
        "bdg_list_top_tse_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_tse_candidaturas", {"termo": "Campinas"}),
        (3, "bdg_get_tse_votacao", {"termo": "3509502"}),
        (4, "bdg_get_tse_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_tse_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_electorate": True},
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
        assert "tse_eleicoes" in text
