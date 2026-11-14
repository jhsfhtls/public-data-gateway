"""Tests for ANATEL Sistema Mosaico Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
    AnatelMosaicoConnector,
    _classify_telecom_infrastructure,
    _compute_telecom_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_anatel_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = AnatelMosaicoConnector()
    if not (conn.silver_dir / "mosaico_estacoes_smp.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "geografico"
        / "anatel_mosaico"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-ANATEL-MOSAICO-001"
    assert data["source_id"] == "anatel_mosaico"
    tables = data["schema"]["tables"]
    assert "mosaico_estacoes_smp" in tables
    assert "mosaico_banda_larga_scm" in tables
    assert "mosaico_espectro_radiofrequencia" in tables
    assert "mosaico_cobertura_setores" in tables
    assert "features_telecom_conectividade_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e tier de conectividade."""
    score_conn, score_5g = _compute_telecom_scores(
        proporcao_5g_pct=34.0,
        densidade_erbs_10k=15.0,
        participacao_fibra_pct=88.0,
        velocidade_mediana_mbps=350.0,
        populacao_coberta_5g_pct=92.0,
        possui_backhaul_fibra=True,
    )
    assert 85.0 <= score_conn <= 100.0
    assert 75.0 <= score_5g <= 100.0
    assert (
        _classify_telecom_infrastructure(score_conn, score_5g, True)
        == "HUB_5G_FIBRA_AVANCADO"
    )
    assert (
        _classify_telecom_infrastructure(74.0, 62.0, True)
        == "CONECTIVIDADE_CONSOLIDADA"
    )
    assert (
        _classify_telecom_infrastructure(58.0, 40.0, False)
        == "EXPANSAO_INFRAESTRUTURA"
    )
    assert _classify_telecom_infrastructure(35.0, 15.0, False) == "GAP_CONECTIVIDADE"


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/anatel_mosaico/."""
    conn = AnatelMosaicoConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_estacoes = conn.silver_dir / "mosaico_estacoes_smp.parquet"
    p_banda = conn.silver_dir / "mosaico_banda_larga_scm.parquet"
    p_espectro = conn.silver_dir / "mosaico_espectro_radiofrequencia.parquet"
    p_cobertura = conn.silver_dir / "mosaico_cobertura_setores.parquet"
    p_gold = conn.gold_dir / "features_telecom_conectividade_municipios_brasil.parquet"

    for path in (p_estacoes, p_banda, p_espectro, p_cobertura, p_gold):
        assert path.exists(), f"Parquet ausente: {path}"

    t_est = pq.read_table(p_estacoes)
    assert len(t_est) >= 12
    assert "total_erbs_licenciadas" in t_est.column_names

    t_esp = pq.read_table(p_espectro)
    assert len(t_esp) >= 48

    t_gold = pq.read_table(p_gold)
    assert len(t_gold) >= 12
    assert "score_conectividade_digital" in t_gold.column_names
    assert "score_maturidade_5g" in t_gold.column_names


def test_connector_methods_and_registry() -> None:
    """Testa métodos de consulta do conector e registro dinâmico por source_id."""
    dynamic_conn = get_connector("anatel_mosaico")
    assert isinstance(dynamic_conn, AnatelMosaicoConnector)

    stations = dynamic_conn.get_station_coverage_summary("3550308")
    assert stations["id_municipio"] == "3550308"
    assert stations["total_erbs_licenciadas"] > 0

    broadband = dynamic_conn.get_broadband_scm_profile("3550308")
    assert broadband["id_municipio"] == "3550308"
    assert broadband["possui_backhaul_fibra"] is True

    spectrum = dynamic_conn.list_spectrum_allocations("3550308", faixa_frequencia_mhz="3500")
    assert len(spectrum) == 1
    assert spectrum[0]["faixa_frequencia_mhz"] == "3500 MHz"

    features = dynamic_conn.get_telecom_features("4106900")
    assert features["id_municipio"] == "4106900"
    assert 0.0 <= features["score_conectividade_digital"] <= 100.0
    assert 0.0 <= features["score_maturidade_5g"] <= 100.0

    ranking = dynamic_conn.list_top_connected_municipalities(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_conectividade_digital"]
        >= ranking[-1]["score_conectividade_digital"]
    )


def test_sdk_parity() -> None:
    """Testa a paridade das chamadas ANATEL no Python SDK BrazilData."""
    sdk = BrazilData()

    st = sdk.get_telecom_stations("3550308")
    assert st["id_municipio"] == "3550308"

    bb = sdk.get_broadband_profile("3550308")
    assert bb["id_municipio"] == "3550308"

    sp = sdk.list_spectrum_allocations("3550308")
    assert len(sp) == 4

    feat = sdk.get_telecom_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_connected_municipalities(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de anatel stations, broadband, spectrum, features e ranking."""
    assert main(["anatel", "stations", "3550308", "--json"]) == 0
    st_data = json.loads(capsys.readouterr().out)
    assert st_data["id_municipio"] == "3550308"

    assert main(["anatel", "broadband", "3550308"]) == 0
    assert "ANATEL Mosaico Banda Larga SCM" in capsys.readouterr().out

    assert main(["anatel", "spectrum", "3550308", "--faixa", "3500"]) == 0
    assert "3500 MHz" in capsys.readouterr().out

    assert main(["anatel", "features", "3550308"]) == 0
    assert "Score Conectividade" in capsys.readouterr().out

    assert main(["anatel", "ranking", "--limit", "3"]) == 0
    assert "Conectividade Digital" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para ANATEL Mosaico."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/geo/anatel/top-connected?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/geo/anatel/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_bb = client.get("/api/v1/geo/anatel/broadband/3550308")
    assert r_bb.status_code == 200
    assert r_bb.json()["id_municipio"] == "3550308"

    r_sp = client.get("/api/v1/geo/anatel/spectrum/3550308?faixa=3500")
    assert r_sp.status_code == 200
    assert len(r_sp.json()) == 1

    r_st = client.get("/api/v1/geo/anatel/stations/3550308")
    assert r_st.status_code == 200
    assert r_st.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/anatel/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP ANATEL e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_anatel_stations",
        "bdg_get_anatel_broadband",
        "bdg_get_anatel_telecom_features",
        "bdg_list_top_connected_municipalities",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_anatel_stations", {"id_municipio": "3550308"}),
        ("bdg_get_anatel_broadband", {"id_municipio": "3550308"}),
        ("bdg_get_anatel_telecom_features", {"id_municipio": "3550308"}),
        ("bdg_list_top_connected_municipalities", {"limit": 2}),
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
        assert "anatel_mosaico" in content
