"""Offline tests and 4-interface parity suite for cgu_beneficios_cidadao connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.cgu_beneficios_cidadao import (
    CguBeneficiosCidadaoConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_cgu_beneficios_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = CguBeneficiosCidadaoConnector(settings=settings)
    registered = get_connector("cgu_beneficios_cidadao", settings=settings)
    assert isinstance(registered, CguBeneficiosCidadaoConnector)
    assert connector.contract.source_id == "cgu_beneficios_cidadao"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "cgu_beneficios_cidadao" / "silver"
    gold_dir = tmp_path / "cgu_beneficios_cidadao" / "gold"
    assert (silver_dir / "cgu_bolsa_familia_pagamentos.parquet").exists()
    assert (silver_dir / "cgu_bpc_loas_beneficios.parquet").exists()
    assert (silver_dir / "cgu_seguro_defeso_safra.parquet").exists()
    assert (silver_dir / "cgu_beneficios_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_cgu_beneficios_cidadao_brasil.parquet").exists()
    assert (tmp_path / "cgu_beneficios_cidadao" / "bronze" / "ingestion_meta.json").exists()

    bf = connector.get_cgu_bolsa_familia_payments("Fortaleza")
    assert len(bf) == 2
    assert bf[0]["familias_beneficiarias_ativas"] == 348200

    bpc = connector.get_cgu_bpc_loas_benefits("2304400")
    assert len(bpc) == 1
    assert bpc[0]["cobertura_cadunico_pobreza_pct"] > 93.0

    rural = connector.get_cgu_defeso_safra_aid("Fortaleza")
    assert rural is not None
    assert rural["pescadores_seguro_defeso_ativos"] == 2940

    summary = connector.get_cgu_beneficios_summary("2304400")
    assert summary is not None
    assert summary["familias_bolsa_familia"] == 348200

    features = connector.get_cgu_beneficios_features("Fortaleza")
    assert features is not None
    assert features["tier_protecao_social_cidadao"] == "REDE_PROTECAO_SOCIAL_AMPLA_AUDITADA"

    top_sp = connector.list_top_cgu_beneficios_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_cgu_beneficios_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK ComplianceFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_cgu_bolsa_familia_payments("São Paulo")) == 2
    assert len(sdk.get_cgu_bpc_loas_benefits("3550308")) == 1

    rural = sdk.get_cgu_defeso_safra_aid("3550308")
    assert rural is not None
    assert rural["alertas_elegibilidade_cgu_pct"] <= 1.5

    summary = sdk.get_cgu_beneficios_summary("São Paulo")
    assert summary is not None
    assert summary["sigla_uf"] == "SP"

    features = sdk.get_cgu_beneficios_features("São Paulo")
    assert features is not None
    assert features["score_eficacia_protecao_social_cgu"] >= 78.0

    top_vol = sdk.list_top_cgu_beneficios_municipalities(limit=3, sort_by_volume=True)
    assert len(top_vol) == 3
    assert (
        top_vol[0]["volume_mensal_transferencias_mil_brl"]
        >= top_vol[1]["volume_mensal_transferencias_mil_brl"]
    )


def test_cgu_beneficios_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg beneficios-cidadao CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_bf = client.get("/api/v1/compliance/beneficios-cidadao/bolsa-familia/Manaus")
    assert r_bf.status_code == 200
    assert len(r_bf.json()) == 2

    r_bp = client.get("/api/v1/compliance/beneficios-cidadao/bpc/1302603")
    assert r_bp.status_code == 200
    assert len(r_bp.json()) == 1

    r_df = client.get("/api/v1/compliance/beneficios-cidadao/defeso-safra/1302603")
    assert r_df.status_code == 200
    assert r_df.json()["pescadores_seguro_defeso_ativos"] == 6840

    r_mn = client.get("/api/v1/compliance/beneficios-cidadao/municipio/Manaus")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "AM"

    r_ft = client.get("/api/v1/compliance/beneficios-cidadao/features/1302603")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_protecao_social_cidadao"] == "REDE_PROTECAO_SOCIAL_AMPLA_AUDITADA"

    r_rk = client.get("/api/v1/compliance/beneficios-cidadao/ranking?limit=3&sort_by_volume=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["beneficios-cidadao", "bolsa-familia", "1302603", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["beneficios-cidadao", "bpc", "1302603", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["beneficios-cidadao", "defeso-safra", "1302603", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "AM"
    assert cli_main(["beneficios-cidadao", "municipio", "1302603", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["familias_bolsa_familia"] == 246800
    assert cli_main(["beneficios-cidadao", "features", "1302603", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_eficacia_protecao_social_cgu"] >= 78.0
    assert cli_main(["beneficios-cidadao", "ranking", "--limit", "3", "--volume", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_cgu_beneficios_mcp_parity(tmp_path: Path) -> None:
    """Validate MCP tool discovery and execution with security boundary wrapping."""
    dispatcher = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))
    listed = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in listed["result"]["tools"]}
    for expected in (
        "bdg_get_cgu_bolsa_familia_payments",
        "bdg_get_cgu_bpc_loas_benefits",
        "bdg_get_cgu_beneficios_features",
        "bdg_list_top_cgu_beneficios_municipalities",
    ):
        assert expected in tool_names

    for req_id, name, arguments in (
        (2, "bdg_get_cgu_bolsa_familia_payments", {"termo": "Campinas"}),
        (3, "bdg_get_cgu_bpc_loas_benefits", {"termo": "3509502"}),
        (4, "bdg_get_cgu_beneficios_features", {"id_municipio_or_name": "Campinas"}),
        (5, "bdg_list_top_cgu_beneficios_municipalities", {"limit": 3, "sort_by_volume": True}),
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
        assert "cgu_beneficios_cidadao" in text
