"""Offline tests and 4-interface parity suite for prf_acidentes connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.geografico.prf_acidentes import PrfAccidentsConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_prf_acidentes_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = PrfAccidentsConnector(settings=settings)
    registered = get_connector("prf_acidentes", settings=settings)
    assert isinstance(registered, PrfAccidentsConnector)
    assert connector.contract.source_id == "prf_acidentes"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "prf_acidentes" / "silver"
    gold_dir = tmp_path / "prf_acidentes" / "gold"
    assert (silver_dir / "prf_sinistros_rodovias_br.parquet").exists()
    assert (silver_dir / "prf_corredores_logisticos_trechos.parquet").exists()
    assert (silver_dir / "prf_perfil_causas_condicoes.parquet").exists()
    assert (silver_dir / "prf_sinistralidade_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_sinistralidade_rodoviaria_prf.parquet").exists()
    assert (tmp_path / "prf_acidentes" / "bronze" / "ingestion_meta.json").exists()

    sinistros = connector.get_prf_accidents("Curitiba")
    assert len(sinistros) == 2
    assert sinistros[0]["rodovia_br"] == "BR-116"

    corredores = connector.get_prf_corridors("4106902")
    assert len(corredores) == 1
    assert corredores[0]["sinistros_carga_pesada_ano"] > 0

    causas = connector.get_prf_causes_profile("Curitiba")
    assert causas is not None
    assert causas["alcoolemia_autuacoes_ano"] > 0

    summary = connector.get_prf_municipal_summary("4106902")
    assert summary is not None
    assert summary["total_sinistros_prf_ano"] == 745

    features = connector.get_prf_features("Curitiba")
    assert features is not None
    assert features["tier_risco_rodoviario_prf"] == "SEGURANCA_VIARIA_ALTA"

    top_sp = connector.list_top_safe_prf_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_prf_acidentes_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK GeoFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_prf_accidents("Campinas")) == 2
    assert len(sdk.get_prf_corridors("3509502")) == 1

    causas = sdk.get_prf_causes_profile("3509502")
    assert causas is not None
    assert causas["pct_envolvimento_caminhao_onibus"] > 0

    summary = sdk.get_prf_municipal_summary("Campinas")
    assert summary is not None
    assert summary["rodovia_br_principal"] == "BR-050"

    features = sdk.get_prf_features("Campinas")
    assert features is not None
    assert features["score_seguranca_viaria_prf"] >= 75.0

    top_fat = sdk.list_top_safe_prf_municipalities(limit=3, sort_by_fatalities=True)
    assert len(top_fat) == 3
    assert top_fat[0]["total_obitos_prf_ano"] >= top_fat[1]["total_obitos_prf_ano"]


def test_prf_acidentes_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg rodovias CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_sin = client.get("/api/v1/geo/rodovias-prf/sinistros/Goiânia")
    assert r_sin.status_code == 200
    assert len(r_sin.json()) == 2

    r_cor = client.get("/api/v1/geo/rodovias-prf/corredores/5208707")
    assert r_cor.status_code == 200
    assert len(r_cor.json()) == 1

    r_cau = client.get("/api/v1/geo/rodovias-prf/causas/Goiânia")
    assert r_cau.status_code == 200
    assert r_cau.json()["alcoolemia_autuacoes_ano"] > 0

    r_mun = client.get("/api/v1/geo/rodovias-prf/municipio/5208707")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/geo/rodovias-prf/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_risco_rodoviario_prf"] == "SEGURANCA_VIARIA_ALTA"

    r_rk = client.get("/api/v1/geo/rodovias-prf/ranking?limit=3&sort_by_fatalities=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["rodovias", "sinistros", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["rodovias", "corredores", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["rodovias", "causas", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["rodovias", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["rodovias", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_seguranca_viaria_prf"] >= 75.0
    assert cli_main(["rodovias", "ranking", "--limit", "3", "--obitos", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_prf_acidentes_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for PRF Acidentes."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_prf_sinistros_rodovias",
        "bdg_get_prf_corredores_logisticos",
        "bdg_get_prf_sinistralidade_features",
        "bdg_list_top_prf_seguranca_viaria",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_prf_sinistros_rodovias", {"termo": "Campinas"}),
        (3, "bdg_get_prf_corredores_logisticos", {"termo": "3509502"}),
        (4, "bdg_get_prf_sinistralidade_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_prf_seguranca_viaria",
            {"limit": 3, "sigla_uf": "SP", "sort_by_fatalities": True},
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
        assert "prf_acidentes" in text
