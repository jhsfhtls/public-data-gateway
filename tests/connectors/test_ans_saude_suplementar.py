"""Offline tests and 4-interface parity suite for ans_saude_suplementar connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.saude.ans_saude_suplementar import (
    AnsSaudeSuplementarConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_ans_saude_suplementar_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = AnsSaudeSuplementarConnector(settings=settings)
    registered = get_connector("ans_saude_suplementar", settings=settings)
    assert isinstance(registered, AnsSaudeSuplementarConnector)
    assert connector.contract.source_id == "ans_saude_suplementar"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "ans_saude_suplementar" / "silver"
    gold_dir = tmp_path / "ans_saude_suplementar" / "gold"
    assert (silver_dir / "ans_beneficiarios_municipio.parquet").exists()
    assert (silver_dir / "ans_operadoras_idss_qualidade.parquet").exists()
    assert (silver_dir / "ans_sinistralidade_diops_economico.parquet").exists()
    assert (silver_dir / "ans_saude_suplementar_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_ans_saude_suplementar_brasil.parquet").exists()
    assert (tmp_path / "ans_saude_suplementar" / "bronze" / "ingestion_meta.json").exists()

    ben = connector.get_beneficiaries("Curitiba")
    assert len(ben) == 2
    assert ben[0]["sigla_uf"] == "PR"

    idss = connector.get_operator_quality_idss("4106902")
    assert len(idss) == 1
    assert idss[0]["idss_medio_ponderado"] > 0.80

    diops = connector.get_diops_loss_ratio("Curitiba")
    assert diops is not None
    assert diops["indice_solvencia_ans_pct"] > 100.0

    summary = connector.get_ans_summary("4106902")
    assert summary is not None
    assert summary["total_beneficiarios_med_odo"] > 1_000_000

    features = connector.get_ans_features("Curitiba")
    assert features is not None
    assert features["tier_saude_suplementar_ans"] == "POLO_ALTA_COBERTURA_ANS"

    top_sp = connector.list_top_ans_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_ans_saude_suplementar_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK HealthFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_ans_beneficiaries("Campinas")) == 2
    assert len(sdk.get_ans_operator_quality("3509502")) == 1

    diops = sdk.get_ans_diops_loss_ratio("3509502")
    assert diops is not None
    assert diops["sinistralidade_assistencial_pct"] > 70.0

    summary = sdk.get_ans_municipal_summary("Campinas")
    assert summary is not None
    assert summary["taxa_cobertura_privada_pct"] > 40.0

    features = sdk.get_ans_features("Campinas")
    assert features is not None
    assert features["score_maturidade_saude_suplementar_ans"] >= 75.0

    top_ben = sdk.list_top_ans_municipalities(limit=3, sort_by_beneficiaries=True)
    assert len(top_ben) == 3
    assert top_ben[0]["total_beneficiarios_med_odo"] >= top_ben[1]["total_beneficiarios_med_odo"]


def test_ans_saude_suplementar_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg ans-suplementar CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_bn = client.get("/api/v1/health/ans-suplementar/beneficiarios/Goiânia")
    assert r_bn.status_code == 200
    assert len(r_bn.json()) == 2

    r_id = client.get("/api/v1/health/ans-suplementar/idss/5208707")
    assert r_id.status_code == 200
    assert len(r_id.json()) == 1

    r_dp = client.get("/api/v1/health/ans-suplementar/diops/Goiânia")
    assert r_dp.status_code == 200
    assert r_dp.json()["indice_solvencia_ans_pct"] > 100.0

    r_mn = client.get("/api/v1/health/ans-suplementar/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/health/ans-suplementar/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_saude_suplementar_ans"] == "POLO_ALTA_COBERTURA_ANS"

    r_rk = client.get("/api/v1/health/ans-suplementar/ranking?limit=3&sort_by_beneficiaries=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["ans-suplementar", "beneficiarios", "Fortaleza", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["ans-suplementar", "idss", "2304400", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["ans-suplementar", "diops", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "CE"
    assert cli_main(["ans-suplementar", "municipio", "2304400", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "2304400"
    assert cli_main(["ans-suplementar", "features", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_maturidade_saude_suplementar_ans"] >= 75.0
    assert cli_main(["ans-suplementar", "ranking", "--limit", "3", "--beneficiarios", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_ans_saude_suplementar_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for ANS Saúde Suplementar."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_ans_beneficiarios",
        "bdg_get_ans_idss_qualidade",
        "bdg_get_ans_features",
        "bdg_list_top_ans_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_ans_beneficiarios", {"termo": "Campinas"}),
        (3, "bdg_get_ans_idss_qualidade", {"termo": "3509502"}),
        (4, "bdg_get_ans_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_ans_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_beneficiaries": True},
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
        assert "ans_saude_suplementar" in text
