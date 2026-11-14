"""Tests for DataSUS CNES Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.saude.datasus_cnes.connector import (
    DataSUSCnesConnector,
    _classify_health_network,
    _compute_diagnostic_index,
    _compute_health_capacity_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_cnes_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = DataSUSCnesConnector()
    if not (conn.silver_dir / "cnes_estabelecimentos_saude.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "saude"
        / "datasus_cnes"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-DATASUS-CNES-001"
    assert data["source_id"] == "datasus_cnes"
    tables = data["schema"]["tables"]
    assert "cnes_estabelecimentos_saude" in tables
    assert "cnes_leitos_hospitalares_uti" in tables
    assert "cnes_equipamentos_diagnostico" in tables
    assert "cnes_profissionais_equipes_saude" in tables
    assert "features_cnes_capacidade_assistencial_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e classificação da rede."""
    idx_diag = _compute_diagnostic_index(
        tomografos=142,
        ressonancia=74,
        mamografos=98,
        hemodialise=410,
        raio_x=315,
    )
    assert 90.0 <= idx_diag <= 100.0

    score_capacidade, score_uti = _compute_health_capacity_scores(
        leitos_por_1000_hab=3.25,
        leitos_uti_por_10k_hab=5.10,
        hospitais_gerais=108,
        proporcao_rede_sus_pct=45.59,
        indice_diagnostico=idx_diag,
    )
    assert 78.0 <= score_capacidade <= 100.0
    assert 85.0 <= score_uti <= 100.0
    assert (
        _classify_health_network(score_capacidade, score_uti, 3170)
        == "POLO_MACRORREGIONAL_ALTA_COMPLEXIDADE"
    )
    assert (
        _classify_health_network(68.0, 65.0, 350)
        == "CENTRO_REFERENCIA_HOSPITALAR_REGIONAL"
    )
    assert _classify_health_network(52.0, 48.0, 180) == "REDE_ASSISTENCIAL_SECUNDARIA"
    assert _classify_health_network(40.0, 35.0, 80) == "ATENCAO_BASICA_PREDOMINANTE"


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/datasus_cnes/."""
    conn = DataSUSCnesConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_estab = conn.silver_dir / "cnes_estabelecimentos_saude.parquet"
    p_leitos = conn.silver_dir / "cnes_leitos_hospitalares_uti.parquet"
    p_equip = conn.silver_dir / "cnes_equipamentos_diagnostico.parquet"
    p_prof = conn.silver_dir / "cnes_profissionais_equipes_saude.parquet"
    p_gold = (
        conn.gold_dir
        / "features_cnes_capacidade_assistencial_municipios_brasil.parquet"
    )

    for path in (p_estab, p_leitos, p_equip, p_prof, p_gold):
        assert path.exists(), f"Parquet ausente: {path}"

    t_estab = pq.read_table(p_estab)
    assert len(t_estab) >= 12
    assert "total_estabelecimentos_ativos" in t_estab.column_names
    assert "cnpj_14" in t_estab.column_names

    t_leitos = pq.read_table(p_leitos)
    assert len(t_leitos) >= 12
    assert "total_leitos_uti" in t_leitos.column_names

    t_prof = pq.read_table(p_prof)
    assert len(t_prof) >= 36

    t_gold = pq.read_table(p_gold)
    assert len(t_gold) >= 12
    assert "score_capacidade_hospitalar" in t_gold.column_names
    assert "score_alta_complexidade_uti" in t_gold.column_names
    assert "classificacao_rede_assistencial" in t_gold.column_names


def test_connector_methods_and_registry() -> None:
    """Testa métodos de consulta do conector e registro dinâmico por source_id."""
    dynamic_conn = get_connector("datasus_cnes")
    assert isinstance(dynamic_conn, DataSUSCnesConnector)

    estab = dynamic_conn.get_establishments_summary("3550308")
    assert estab["id_municipio"] == "3550308"
    assert estab["total_estabelecimentos_ativos"] > 0

    beds = dynamic_conn.get_hospital_beds_icu("3550308")
    assert beds["id_municipio"] == "3550308"
    assert beds["total_leitos_uti"] > 0

    equip = dynamic_conn.get_diagnostic_equipment("3550308")
    assert equip["id_municipio"] == "3550308"
    assert equip["tomografos_computadorizados"] > 0

    workforce = dynamic_conn.list_health_workforce(
        "3550308", categoria_profissional="Medicos"
    )
    assert len(workforce) == 1
    assert workforce[0]["categoria_profissional"] == "Medicos"

    features = dynamic_conn.get_health_capacity_features("3304557")
    assert features["id_municipio"] == "3304557"
    assert 0.0 <= features["score_capacidade_hospitalar"] <= 100.0
    assert 0.0 <= features["score_alta_complexidade_uti"] <= 100.0

    ranking = dynamic_conn.list_top_health_hubs(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_capacidade_hospitalar"]
        >= ranking[-1]["score_capacidade_hospitalar"]
    )


def test_sdk_parity() -> None:
    """Testa a paridade das chamadas DataSUS CNES no Python SDK BrazilData."""
    sdk = BrazilData()

    est = sdk.get_cnes_establishments("3550308")
    assert est["id_municipio"] == "3550308"

    beds = sdk.get_cnes_beds_icu("3550308")
    assert beds["id_municipio"] == "3550308"

    eq = sdk.get_cnes_equipment("3550308")
    assert eq["id_municipio"] == "3550308"

    wf = sdk.list_cnes_workforce("3550308")
    assert len(wf) == 3

    feat = sdk.get_cnes_health_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_health_hubs(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de cnes establishments, beds, equipment, workforce, features e ranking."""
    assert main(["cnes", "establishments", "3550308", "--json"]) == 0
    est_data = json.loads(capsys.readouterr().out)
    assert est_data["id_municipio"] == "3550308"

    assert main(["cnes", "beds", "3550308"]) == 0
    assert "DataSUS CNES Leitos & UTI" in capsys.readouterr().out

    assert main(["cnes", "equipment", "3550308"]) == 0
    assert "DataSUS CNES Equipamentos Diagnósticos" in capsys.readouterr().out

    assert main(["cnes", "workforce", "3550308", "--categoria", "Medicos"]) == 0
    assert "Medicos" in capsys.readouterr().out

    assert main(["cnes", "features", "3550308"]) == 0
    assert "Score Capacidade Hosp" in capsys.readouterr().out

    assert main(["cnes", "ranking", "--limit", "3"]) == 0
    assert "Capacidade Assistencial" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para DataSUS CNES."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/health-public/cnes/top-hubs?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/health-public/cnes/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_bed = client.get("/api/v1/health-public/cnes/beds/3550308")
    assert r_bed.status_code == 200
    assert r_bed.json()["id_municipio"] == "3550308"

    r_eq = client.get("/api/v1/health-public/cnes/equipment/3550308")
    assert r_eq.status_code == 200
    assert r_eq.json()["id_municipio"] == "3550308"

    r_wf = client.get("/api/v1/health-public/cnes/workforce/3550308?categoria=Medicos")
    assert r_wf.status_code == 200
    assert len(r_wf.json()) == 1

    r_est = client.get("/api/v1/health-public/cnes/establishments/3550308")
    assert r_est.status_code == 200
    assert r_est.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/cnes/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP DataSUS CNES e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_cnes_establishments",
        "bdg_get_cnes_beds_icu",
        "bdg_get_cnes_health_features",
        "bdg_list_top_health_hubs",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_cnes_establishments", {"id_municipio": "3550308"}),
        ("bdg_get_cnes_beds_icu", {"id_municipio": "3550308"}),
        ("bdg_get_cnes_health_features", {"id_municipio": "3550308"}),
        ("bdg_list_top_health_hubs", {"limit": 2}),
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
        assert "datasus_cnes" in content
