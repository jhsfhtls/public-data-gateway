"""Tests for ANTT Malha de Transporte Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
    AnttMalhaTransporteConnector,
    _classify_logistics_node,
    _compute_logistics_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_antt_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = AnttMalhaTransporteConnector()
    if not (conn.silver_dir / "antt_corredores_rodoviarios.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "geografico"
        / "antt_malha_transporte"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-ANTT-MALHA-001"
    assert data["source_id"] == "antt_malha_transporte"
    tables = data["schema"]["tables"]
    assert "antt_corredores_rodoviarios" in tables
    assert "antt_frota_rntrc_municipio" in tables
    assert "antt_malha_ferroviaria_terminais" in tables
    assert "antt_fluxo_logistico_modal" in tables
    assert "features_malha_transporte_logistica_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e classificação logística."""
    score_infra, score_intermodal = _compute_logistics_scores(
        extensao_federal_km=342.5,
        proporcao_concedida_pct=87.0,
        vmd_pesados=28400,
        frota_habilitados=148500,
        possui_ferrovia=True,
        terminais_intermodal=6,
        share_ferrovia_pct=29.0,
        share_hidroviario_pct=7.0,
    )
    assert 85.0 <= score_infra <= 100.0
    assert 80.0 <= score_intermodal <= 100.0
    assert (
        _classify_logistics_node(score_infra, score_intermodal, True)
        == "HUB_LOGISTICO_INTERMODAL_NACIONAL"
    )
    assert (
        _classify_logistics_node(68.0, 52.0, True)
        == "CORREDOR_RODOFERROVIARIO_CONSOLIDADO"
    )
    assert _classify_logistics_node(48.0, 35.0, False) == "POLO_RODOVIARIO_REGIONAL"
    assert _classify_logistics_node(25.0, 20.0, False) == "ACESSO_LOGISTICO_SECUNDARIO"


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/antt_malha_transporte/."""
    conn = AnttMalhaTransporteConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_corredores = conn.silver_dir / "antt_corredores_rodoviarios.parquet"
    p_rntrc = conn.silver_dir / "antt_frota_rntrc_municipio.parquet"
    p_ferrovia = conn.silver_dir / "antt_malha_ferroviaria_terminais.parquet"
    p_modais = conn.silver_dir / "antt_fluxo_logistico_modal.parquet"
    p_gold = (
        conn.gold_dir
        / "features_malha_transporte_logistica_municipios_brasil.parquet"
    )

    for path in (p_corredores, p_rntrc, p_ferrovia, p_modais, p_gold):
        assert path.exists(), f"Parquet ausente: {path}"

    t_corr = pq.read_table(p_corredores)
    assert len(t_corr) >= 12
    assert "extensao_malha_federal_km" in t_corr.column_names

    t_mod = pq.read_table(p_modais)
    assert len(t_mod) >= 36

    t_gold = pq.read_table(p_gold)
    assert len(t_gold) >= 12
    assert "score_infraestrutura_logistica" in t_gold.column_names
    assert "score_intermodalidade_cargas" in t_gold.column_names
    assert "classificacao_nodo_logistico" in t_gold.column_names


def test_connector_methods_and_registry() -> None:
    """Testa métodos de consulta do conector e registro dinâmico por source_id."""
    dynamic_conn = get_connector("antt_malha_transporte")
    assert isinstance(dynamic_conn, AnttMalhaTransporteConnector)

    corr = dynamic_conn.get_highway_corridors("3550308")
    assert corr["id_municipio"] == "3550308"
    assert corr["extensao_malha_federal_km"] > 0

    rntrc = dynamic_conn.get_rntrc_fleet_profile("3550308")
    assert rntrc["id_municipio"] == "3550308"
    assert rntrc["frota_veiculos_habilitados"] > 0

    rail = dynamic_conn.get_rail_intermodal_terminals("3550308")
    assert rail["id_municipio"] == "3550308"
    assert rail["possui_conexao_ferroviaria"] is True

    modais = dynamic_conn.list_modal_flows("3550308", modal_transporte="Ferroviario")
    assert len(modais) == 1
    assert modais[0]["modal_transporte"] == "Ferroviario"

    features = dynamic_conn.get_logistics_features("4106900")
    assert features["id_municipio"] == "4106900"
    assert 0.0 <= features["score_infraestrutura_logistica"] <= 100.0
    assert 0.0 <= features["score_intermodalidade_cargas"] <= 100.0

    ranking = dynamic_conn.list_top_logistics_hubs(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_infraestrutura_logistica"]
        >= ranking[-1]["score_infraestrutura_logistica"]
    )


def test_sdk_parity() -> None:
    """Testa a paridade das chamadas ANTT no Python SDK BrazilData."""
    sdk = BrazilData()

    hw = sdk.get_highway_corridors("3550308")
    assert hw["id_municipio"] == "3550308"

    rn = sdk.get_rntrc_fleet_profile("3550308")
    assert rn["id_municipio"] == "3550308"

    rl = sdk.get_rail_intermodal_terminals("3550308")
    assert rl["id_municipio"] == "3550308"

    md = sdk.list_modal_flows("3550308")
    assert len(md) == 3

    feat = sdk.get_logistics_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_logistics_hubs(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de antt highways, rntrc, rail, modals, features e ranking."""
    assert main(["antt", "highways", "3550308", "--json"]) == 0
    hw_data = json.loads(capsys.readouterr().out)
    assert hw_data["id_municipio"] == "3550308"

    assert main(["antt", "rntrc", "3550308"]) == 0
    assert "ANTT Cadastro RNTRC" in capsys.readouterr().out

    assert main(["antt", "rail", "3550308"]) == 0
    assert "ANTT Malha Ferroviária" in capsys.readouterr().out

    assert main(["antt", "modals", "3550308", "--modal", "Ferroviario"]) == 0
    assert "Ferroviario" in capsys.readouterr().out

    assert main(["antt", "features", "3550308"]) == 0
    assert "Score Infra Logística" in capsys.readouterr().out

    assert main(["antt", "ranking", "--limit", "3"]) == 0
    assert "Infraestrutura Logística" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para ANTT Malha de Transporte."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/geo/antt/top-hubs?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/geo/antt/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_rn = client.get("/api/v1/geo/antt/rntrc/3550308")
    assert r_rn.status_code == 200
    assert r_rn.json()["id_municipio"] == "3550308"

    r_rl = client.get("/api/v1/geo/antt/rail/3550308")
    assert r_rl.status_code == 200
    assert r_rl.json()["id_municipio"] == "3550308"

    r_md = client.get("/api/v1/geo/antt/modals/3550308?modal=Ferroviario")
    assert r_md.status_code == 200
    assert len(r_md.json()) == 1

    r_hw = client.get("/api/v1/geo/antt/highways/3550308")
    assert r_hw.status_code == 200
    assert r_hw.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/antt/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP ANTT e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_antt_highway_corridors",
        "bdg_get_antt_rntrc_fleet",
        "bdg_get_antt_logistics_features",
        "bdg_list_top_logistics_hubs",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_antt_highway_corridors", {"id_municipio": "3550308"}),
        ("bdg_get_antt_rntrc_fleet", {"id_municipio": "3550308"}),
        ("bdg_get_antt_logistics_features", {"id_municipio": "3550308"}),
        ("bdg_list_top_logistics_hubs", {"limit": 2}),
    ):
        req_call = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        res_call = dispatcher.handle_request(req_call)
        assert not res_call.get("isError")
        content = res_call["result"]["content"][0]["text"]
        assert "untrusted_gov_data" in content
        assert "antt_malha_transporte" in content
