"""Tests for DataSUS SIH/SUS Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.saude.datasus_sih.connector import (
    DataSUSSihConnector,
    _classify_sih_profile,
    _compute_sih_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_sih_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = DataSUSSihConnector()
    if not (conn.silver_dir / "sih_internacoes_aih_municipio.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "saude"
        / "datasus_sih"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-DATASUS-SIH-001"
    assert data["source_id"] == "datasus_sih"
    tables = data["schema"]["tables"]
    assert "sih_internacoes_aih_municipio" in tables
    assert "sih_custos_hospitalares_sus" in tables
    assert "sih_morbidade_capitulos_cid10" in tables
    assert "sih_desfechos_mortalidade_hospitalar" in tables
    assert "features_sih_morbidade_custos_hospitalares_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e perfil epidemiológico SIH."""
    score_complexidade, score_pressao = _compute_sih_scores(
        ticket_medio_aih_brl=2974.17,
        proporcao_custo_uti_pct=31.0,
        taxa_utilizacao_uti_pct=14.5,
        internacoes_por_1000_hab_ano=56.4,
        proporcao_urgencia_pct=76.0,
        proporcao_icsap_pct=15.0,
    )
    assert 85.0 <= score_complexidade <= 100.0
    assert 65.0 <= score_pressao <= 80.0
    assert (
        _classify_sih_profile(score_complexidade, score_pressao, 31.0)
        == "POLO_CIRURGICO_ALTA_COMPLEXIDADE_NACIONAL"
    )
    assert (
        _classify_sih_profile(72.0, 76.0, 24.4)
        == "CENTRO_REFERENCIA_MORBIDADE_REGIONAL"
    )
    assert (
        _classify_sih_profile(54.0, 82.0, 18.0)
        == "REDE_HOSPITALAR_MEDIA_COMPLEXIDADE"
    )
    assert (
        _classify_sih_profile(42.2, 89.8, 14.2)
        == "PERFIL_CLINICO_BAIXA_COMPLEXIDADE_ICSAP"
    )


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/datasus_sih/."""
    conn = DataSUSSihConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_int = conn.silver_dir / "sih_internacoes_aih_municipio.parquet"
    p_cus = conn.silver_dir / "sih_custos_hospitalares_sus.parquet"
    p_cid = conn.silver_dir / "sih_morbidade_capitulos_cid10.parquet"
    p_des = conn.silver_dir / "sih_desfechos_mortalidade_hospitalar.parquet"
    p_gold = (
        conn.gold_dir
        / "features_sih_morbidade_custos_hospitalares_municipios_brasil.parquet"
    )

    for path in (p_int, p_cus, p_cid, p_des, p_gold):
        assert path.exists(), f"Parquet ausente: {path}"

    t_int = pq.read_table(p_int)
    assert len(t_int) >= 12
    assert "total_internacoes_aih" in t_int.column_names

    t_cus = pq.read_table(p_cus)
    assert len(t_cus) >= 12
    assert "valor_total_aih_brl" in t_cus.column_names

    t_cid = pq.read_table(p_cid)
    assert len(t_cid) >= 36

    t_gold = pq.read_table(p_gold)
    assert len(t_gold) >= 12
    assert "score_complexidade_assistencial_sih" in t_gold.column_names
    assert "score_pressao_morbidade_hospitalar" in t_gold.column_names
    assert "perfil_epidemiologico_hospitalar" in t_gold.column_names


def test_connector_methods_and_registry() -> None:
    """Testa métodos de consulta do conector e registro dinâmico por source_id."""
    dynamic_conn = get_connector("datasus_sih")
    assert isinstance(dynamic_conn, DataSUSSihConnector)

    adm = dynamic_conn.get_admissions_summary("3550308")
    assert adm["id_municipio"] == "3550308"
    assert adm["total_internacoes_aih"] > 0

    costs = dynamic_conn.get_hospital_costs("3550308")
    assert costs["id_municipio"] == "3550308"
    assert costs["valor_total_aih_brl"] > 0

    cid10 = dynamic_conn.list_cid10_morbidity(
        "3550308", capitulo_cid10="Circulatorio"
    )
    assert len(cid10) == 1
    assert "Circulatorio" in cid10[0]["capitulo_cid10"]

    outcomes = dynamic_conn.get_mortality_outcomes("3550308")
    assert outcomes["id_municipio"] == "3550308"
    assert outcomes["obitos_hospitalares"] > 0

    features = dynamic_conn.get_sih_morbidity_features("4314902")
    assert features["id_municipio"] == "4314902"
    assert 0.0 <= features["score_complexidade_assistencial_sih"] <= 100.0
    assert 0.0 <= features["score_pressao_morbidade_hospitalar"] <= 100.0

    ranking = dynamic_conn.list_top_sih_complexity_hubs(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_complexidade_assistencial_sih"]
        >= ranking[-1]["score_complexidade_assistencial_sih"]
    )


def test_sdk_parity() -> None:
    """Testa a paridade das chamadas DataSUS SIH no Python SDK BrazilData."""
    sdk = BrazilData()

    adm = sdk.get_sih_admissions("3550308")
    assert adm["id_municipio"] == "3550308"

    costs = sdk.get_sih_hospital_costs("3550308")
    assert costs["id_municipio"] == "3550308"

    cid = sdk.list_sih_cid10_morbidity("3550308")
    assert len(cid) == 3

    out = sdk.get_sih_mortality_outcomes("3550308")
    assert out["id_municipio"] == "3550308"

    feat = sdk.get_sih_morbidity_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_sih_complexity_hubs(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de sih admissions, costs, cid10, outcomes, features e ranking."""
    assert main(["sih", "admissions", "3550308", "--json"]) == 0
    adm_data = json.loads(capsys.readouterr().out)
    assert adm_data["id_municipio"] == "3550308"

    assert main(["sih", "costs", "3550308"]) == 0
    assert "DataSUS SIH Custos Hospitalares" in capsys.readouterr().out

    assert main(["sih", "cid10", "3550308", "--capitulo", "Circulatorio"]) == 0
    assert "Circulatorio" in capsys.readouterr().out

    assert main(["sih", "outcomes", "3550308"]) == 0
    assert "DataSUS SIH Desfechos Hospitalares" in capsys.readouterr().out

    assert main(["sih", "features", "3550308"]) == 0
    assert "Score Complexidade SIH" in capsys.readouterr().out

    assert main(["sih", "ranking", "--limit", "3"]) == 0
    assert "Complexidade Hospitalar" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para DataSUS SIH/SUS."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/health-public/sih/top-hubs?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/health-public/sih/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_cost = client.get("/api/v1/health-public/sih/costs/3550308")
    assert r_cost.status_code == 200
    assert r_cost.json()["id_municipio"] == "3550308"

    r_cid = client.get("/api/v1/health-public/sih/cid10/3550308?capitulo=Circulatorio")
    assert r_cid.status_code == 200
    assert len(r_cid.json()) == 1

    r_out = client.get("/api/v1/health-public/sih/outcomes/3550308")
    assert r_out.status_code == 200
    assert r_out.json()["id_municipio"] == "3550308"

    r_adm = client.get("/api/v1/health-public/sih/admissions/3550308")
    assert r_adm.status_code == 200
    assert r_adm.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/sih/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP DataSUS SIH e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_sih_admissions",
        "bdg_get_sih_hospital_costs",
        "bdg_get_sih_morbidity_features",
        "bdg_list_top_sih_complexity_hubs",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_sih_admissions", {"id_municipio": "3550308"}),
        ("bdg_get_sih_hospital_costs", {"id_municipio": "3550308"}),
        ("bdg_get_sih_morbidity_features", {"id_municipio": "3550308"}),
        ("bdg_list_top_sih_complexity_hubs", {"limit": 2}),
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
        assert "datasus_sih" in content
