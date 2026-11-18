"""Offline tests and 4-interface parity suite for mps_inss_beneficios connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
    MpsInssBeneficiosConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_mps_inss_beneficios_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = MpsInssBeneficiosConnector(settings=settings)
    registered = get_connector("mps_inss_beneficios", settings=settings)
    assert isinstance(registered, MpsInssBeneficiosConnector)
    assert connector.contract.source_id == "mps_inss_beneficios"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "mps_inss_beneficios" / "silver"
    gold_dir = tmp_path / "mps_inss_beneficios" / "gold"
    assert (silver_dir / "inss_beneficios_emitidos_especie.parquet").exists()
    assert (silver_dir / "inss_beneficios_concedidos_concessoes.parquet").exists()
    assert (silver_dir / "mps_arrecadacao_previdenciaria_gps.parquet").exists()
    assert (silver_dir / "inss_previdencia_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_previdencia_social_inss.parquet").exists()
    assert (tmp_path / "mps_inss_beneficios" / "bronze" / "ingestion_meta.json").exists()

    ben = connector.get_inss_issued_benefits("Curitiba")
    assert len(ben) == 2
    assert ben[0]["sigla_uf"] == "PR"

    conc = connector.get_inss_benefit_grants("4106902")
    assert len(conc) == 1
    assert conc[0]["qtd_concessoes_ano"] > 0

    arr = connector.get_mps_social_contributions("Curitiba")
    assert arr is not None
    assert arr["razao_contribuintes_beneficiarios"] > 1.5

    summary = connector.get_inss_municipal_summary("4106902")
    assert summary is not None
    assert summary["beneficios_inss_emitidos"] == 312000

    features = connector.get_inss_features("Curitiba")
    assert features is not None
    assert features["tier_sustentabilidade_previdenciaria"] == "PREVIDENCIA_SUPERAVITARIA_FORMAL"

    top_sp = connector.list_top_inss_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_mps_inss_beneficios_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK LaborFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_inss_issued_benefits("Campinas")) == 2
    assert len(sdk.get_inss_benefit_grants("3509502")) == 1

    arr = sdk.get_mps_social_contributions("3509502")
    assert arr is not None
    assert arr["arrecadacao_previdenciaria_mil_brl"] > 0

    summary = sdk.get_inss_municipal_summary("Campinas")
    assert summary is not None
    assert summary["ticket_medio_beneficio_brl"] > 2000.0

    features = sdk.get_inss_features("Campinas")
    assert features is not None
    assert features["score_cobertura_previdenciaria_inss"] >= 75.0

    top_vol = sdk.list_top_inss_municipalities(limit=3, sort_by_volume=True)
    assert len(top_vol) == 3
    assert top_vol[0]["valor_mensal_beneficios_mil_brl"] >= top_vol[1]["valor_mensal_beneficios_mil_brl"]


def test_mps_inss_beneficios_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg previdencia CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_ben = client.get("/api/v1/labor/previdencia-inss/beneficios/Goiânia")
    assert r_ben.status_code == 200
    assert len(r_ben.json()) == 2

    r_con = client.get("/api/v1/labor/previdencia-inss/concessoes/5208707")
    assert r_con.status_code == 200
    assert len(r_con.json()) == 1

    r_ctr = client.get("/api/v1/labor/previdencia-inss/contribuicoes/Goiânia")
    assert r_ctr.status_code == 200
    assert r_ctr.json()["razao_contribuintes_beneficiarios"] > 1.0

    r_mun = client.get("/api/v1/labor/previdencia-inss/municipio/5208707")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/labor/previdencia-inss/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_sustentabilidade_previdenciaria"] == "PREVIDENCIA_SUPERAVITARIA_FORMAL"

    r_rk = client.get("/api/v1/labor/previdencia-inss/ranking?limit=3&sort_by_volume=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["previdencia", "beneficios", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["previdencia", "concessoes", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["previdencia", "contribuicoes", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["previdencia", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["previdencia", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_cobertura_previdenciaria_inss"] >= 75.0
    assert cli_main(["previdencia", "ranking", "--limit", "3", "--volume", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_mps_inss_beneficios_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for MPS/INSS."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_mps_inss_beneficios",
        "bdg_get_mps_inss_concessoes",
        "bdg_get_mps_inss_features",
        "bdg_list_top_mps_inss_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_mps_inss_beneficios", {"termo": "Campinas"}),
        (3, "bdg_get_mps_inss_concessoes", {"termo": "3509502"}),
        (4, "bdg_get_mps_inss_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_mps_inss_municipalities",
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
        assert "mps_inss_beneficios" in text
