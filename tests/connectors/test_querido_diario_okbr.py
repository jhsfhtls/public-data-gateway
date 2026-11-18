"""Offline tests and 4-interface parity suite for querido_diario_okbr connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
    QueridoDiarioOkbrConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_querido_diario_okbr_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = QueridoDiarioOkbrConnector(settings=settings)
    registered = get_connector("querido_diario_okbr", settings=settings)
    assert isinstance(registered, QueridoDiarioOkbrConnector)
    assert connector.contract.source_id == "querido_diario_okbr"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "querido_diario_okbr" / "silver"
    gold_dir = tmp_path / "querido_diario_okbr" / "gold"
    assert (silver_dir / "okbr_diarios_licitacoes_dispensas.parquet").exists()
    assert (silver_dir / "okbr_diarios_excertos_tematicos.parquet").exists()
    assert (silver_dir / "okbr_diarios_transparencia_ativa.parquet").exists()
    assert (silver_dir / "okbr_querido_diario_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_querido_diario_okbr_brasil.parquet").exists()
    assert (tmp_path / "querido_diario_okbr" / "bronze" / "ingestion_meta.json").exists()

    lic = connector.get_gazette_tenders("Curitiba")
    assert len(lic) == 2
    assert lic[0]["sigla_uf"] == "PR"

    exc = connector.get_thematic_excerpts("4106902")
    assert len(exc) == 1
    assert exc[0]["taxa_ocr_legivel_pct"] > 90.0

    trp = connector.get_active_transparency_metrics("Curitiba")
    assert trp is not None
    assert trp["indice_transparencia_diario_pct"] > 90.0

    summary = connector.get_gazette_summary("4106902")
    assert summary is not None
    assert summary["qtd_atos_licitatorios_12m"] > 1000

    features = connector.get_gazette_features("Curitiba")
    assert features is not None
    assert features["tier_maturidade_diario_oficial"] == "DIARIO_OFICIAL_EXEMPLAR"

    top_sp = connector.list_top_gazette_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_querido_diario_okbr_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK ProcurementFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_querido_diario_tenders("Campinas")) == 2
    assert len(sdk.get_querido_diario_excerpts("3509502")) == 1

    trp = sdk.get_querido_diario_transparency("3509502")
    assert trp is not None
    assert trp["regularidade_publicacao_pct"] >= 95.0

    summary = sdk.get_querido_diario_summary("Campinas")
    assert summary is not None
    assert summary["edicoes_indexadas_12m"] > 200

    features = sdk.get_querido_diario_features("Campinas")
    assert features is not None
    assert features["score_transparencia_diario_oficial"] >= 78.0

    top_vol = sdk.list_top_querido_diario_municipalities(limit=3, sort_by_volume=True)
    assert len(top_vol) == 3
    assert top_vol[0]["valor_extratos_citados_mil_brl"] >= top_vol[1]["valor_extratos_citados_mil_brl"]


def test_querido_diario_okbr_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg querido-diario CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_lc = client.get("/api/v1/procurement/querido-diario/licitacoes/Goiânia")
    assert r_lc.status_code == 200
    assert len(r_lc.json()) == 2

    r_ex = client.get("/api/v1/procurement/querido-diario/excertos/5208707")
    assert r_ex.status_code == 200
    assert len(r_ex.json()) == 1

    r_tr = client.get("/api/v1/procurement/querido-diario/transparencia/Goiânia")
    assert r_tr.status_code == 200
    assert r_tr.json()["indice_transparencia_diario_pct"] > 80.0

    r_mn = client.get("/api/v1/procurement/querido-diario/municipio/5208707")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/procurement/querido-diario/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_maturidade_diario_oficial"] == "DIARIO_OFICIAL_EXEMPLAR"

    r_rk = client.get("/api/v1/procurement/querido-diario/ranking?limit=3&sort_by_volume=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["querido-diario", "licitacoes", "Fortaleza", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["querido-diario", "excertos", "2304400", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["querido-diario", "transparencia", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "CE"
    assert cli_main(["querido-diario", "municipio", "2304400", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "2304400"
    assert cli_main(["querido-diario", "features", "Fortaleza", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_transparencia_diario_oficial"] >= 78.0
    assert cli_main(["querido-diario", "ranking", "--limit", "3", "--volume", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_querido_diario_okbr_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for Querido Diário OKBR."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_querido_diario_licitacoes",
        "bdg_get_querido_diario_excertos",
        "bdg_get_querido_diario_features",
        "bdg_list_top_querido_diario_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_querido_diario_licitacoes", {"termo": "Campinas"}),
        (3, "bdg_get_querido_diario_excertos", {"termo": "3509502"}),
        (4, "bdg_get_querido_diario_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_querido_diario_municipalities",
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
        assert "querido_diario_okbr" in text
