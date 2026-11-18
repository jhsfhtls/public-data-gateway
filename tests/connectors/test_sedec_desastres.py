"""Offline tests and 4-interface parity suite for sedec_desastres connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.sedec_desastres import SedecDesastresConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_sedec_desastres_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = SedecDesastresConnector(settings=settings)
    registered = get_connector("sedec_desastres", settings=settings)
    assert isinstance(registered, SedecDesastresConnector)
    assert connector.contract.source_id == "sedec_desastres"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "sedec_desastres" / "silver"
    gold_dir = tmp_path / "sedec_desastres" / "gold"
    assert (silver_dir / "sedec_ocorrencias_desastres.parquet").exists()
    assert (silver_dir / "sedec_decretos_reconhecimento.parquet").exists()
    assert (silver_dir / "sedec_cemaden_monitoramento.parquet").exists()
    assert (silver_dir / "sedec_desastres_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_risco_climatico_sedec.parquet").exists()
    assert (tmp_path / "sedec_desastres" / "bronze" / "ingestion_meta.json").exists()

    ocorrencias = connector.get_sedec_disasters("Curitiba")
    assert len(ocorrencias) == 2
    assert ocorrencias[0]["sigla_uf"] == "PR"

    decretos = connector.get_sedec_decrees("4106902")
    assert len(decretos) == 1
    assert decretos[0]["recursos_federais_liberados_mil_brl"] > 0

    cemaden = connector.get_sedec_cemaden_monitoring("Curitiba")
    assert cemaden is not None
    assert cemaden["possui_plano_contingencia_ativo"] is True

    summary = connector.get_sedec_municipal_summary("4106902")
    assert summary is not None
    assert summary["qtd_desastres_s2id_5a"] == 6

    features = connector.get_sedec_features("Curitiba")
    assert features is not None
    assert features["tier_vulnerabilidade_climatica_s2id"] == "BAIXA_VULNERABILIDADE_DESASTRES"

    top_sp = connector.list_top_resilient_sedec_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_sedec_desastres_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK EnvironmentFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_sedec_disasters("Campinas")) == 2
    assert len(sdk.get_sedec_decrees("3509502")) == 1

    cem = sdk.get_sedec_cemaden_monitoring("3509502")
    assert cem is not None
    assert cem["qtd_pluviometros_cemaden"] > 0

    summary = sdk.get_sedec_municipal_summary("Campinas")
    assert summary is not None
    assert summary["prejuizos_economicos_mil_brl"] > 0

    features = sdk.get_sedec_features("Campinas")
    assert features is not None
    assert features["score_resiliencia_desastres_sedec"] >= 75.0

    top_dmg = sdk.list_top_resilient_sedec_municipalities(limit=3, sort_by_damages=True)
    assert len(top_dmg) == 3
    assert top_dmg[0]["prejuizos_economicos_mil_brl"] >= top_dmg[1]["prejuizos_economicos_mil_brl"]


def test_sedec_desastres_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg desastres CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_oc = client.get("/api/v1/environment/desastres/ocorrencias/Goiânia")
    assert r_oc.status_code == 200
    assert len(r_oc.json()) == 2

    r_dec = client.get("/api/v1/environment/desastres/decretos/5208707")
    assert r_dec.status_code == 200
    assert len(r_dec.json()) == 1

    r_cem = client.get("/api/v1/environment/desastres/cemaden/Goiânia")
    assert r_cem.status_code == 200
    assert r_cem.json()["qtd_pluviometros_cemaden"] > 0

    r_mun = client.get("/api/v1/environment/desastres/municipio/5208707")
    assert r_mun.status_code == 200
    assert r_mun.json()["sigla_uf"] == "GO"

    r_ft = client.get("/api/v1/environment/desastres/features/Goiânia")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_vulnerabilidade_climatica_s2id"] == "BAIXA_VULNERABILIDADE_DESASTRES"

    r_rk = client.get("/api/v1/environment/desastres/ranking?limit=3&sort_by_damages=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["desastres", "ocorrencias", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["desastres", "decretos", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["desastres", "cemaden", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["desastres", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["desastres", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_resiliencia_desastres_sedec"] >= 75.0
    assert cli_main(["desastres", "ranking", "--limit", "3", "--prejuizos", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_sedec_desastres_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for SEDEC Desastres."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_sedec_desastres_ocorrencias",
        "bdg_get_sedec_decretos_reconhecimento",
        "bdg_get_sedec_desastres_features",
        "bdg_list_top_sedec_resilient_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_sedec_desastres_ocorrencias", {"termo": "Campinas"}),
        (3, "bdg_get_sedec_decretos_reconhecimento", {"termo": "3509502"}),
        (4, "bdg_get_sedec_desastres_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_sedec_resilient_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_damages": True},
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
        assert "sedec_desastres" in text
