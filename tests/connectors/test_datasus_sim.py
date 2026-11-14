"""Tests for DataSUS SIM Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.saude.datasus_sim.connector import (
    DataSUSSimConnector,
    _classify_sim_profile,
    _compute_sim_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_sim_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = DataSUSSimConnector()
    if not (conn.silver_dir / "sim_obitos_gerais_municipio.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "saude"
        / "datasus_sim"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-DATASUS-SIM-001"
    assert data["source_id"] == "datasus_sim"
    tables = data["schema"]["tables"]
    assert "sim_obitos_gerais_municipio" in tables
    assert "sim_causas_capitulos_cid10" in tables
    assert "sim_mortalidade_materno_infantil" in tables
    assert "sim_mortalidade_prematura_dcnt_externas" in tables
    assert "features_sim_mortalidade_causas_cid10_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e perfil epidemiológico SIM."""
    score_vig, score_carga = _compute_sim_scores(
        proporcao_causas_mal_definidas_pct=2.20,
        proporcao_obitos_infantis_investigados_pct=96.4,
        taxa_mortalidade_infantil_1000_nv=8.2,
        taxa_mortalidade_causas_externas_100k_hab=41.5,
        anos_potenciais_vida_perdidos_1000_hab=48.2,
    )
    assert 90.0 <= score_vig <= 100.0
    assert 45.0 <= score_carga <= 60.0
    assert (
        _classify_sim_profile(score_vig, 8.2)
        == "TRANSICAO_AVANCADA_ALTA_VIGILANCIA_SIM"
    )
    assert (
        _classify_sim_profile(82.0, 10.8)
        == "TRANSICAO_CONSOLIDADA_PREDOMINIO_DCNT"
    )
    assert (
        _classify_sim_profile(66.6, 13.1)
        == "TRANSICAO_INTERMEDIARIA_DUPLA_CARGA"
    )
    assert (
        _classify_sim_profile(37.3, 16.4)
        == "ALTA_PRESSAO_CAUSAS_EXTERNAS_E_EVITAVEIS"
    )


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/datasus_sim/."""
    conn = DataSUSSimConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_gen = conn.silver_dir / "sim_obitos_gerais_municipio.parquet"
    p_cid = conn.silver_dir / "sim_causas_capitulos_cid10.parquet"
    p_mat = conn.silver_dir / "sim_mortalidade_materno_infantil.parquet"
    p_pre = conn.silver_dir / "sim_mortalidade_prematura_dcnt_externas.parquet"
    p_gold = (
        conn.gold_dir
        / "features_sim_mortalidade_causas_cid10_municipios_brasil.parquet"
    )

    for path in (p_gen, p_cid, p_mat, p_pre, p_gold):
        assert path.exists(), f"Parquet ausente: {path}"

    t_gen = pq.read_table(p_gen)
    assert len(t_gen) >= 12
    assert "total_obitos_registrados" in t_gen.column_names

    t_cid = pq.read_table(p_cid)
    assert len(t_cid) >= 48
    assert "capitulo_cid10" in t_cid.column_names

    t_mat = pq.read_table(p_mat)
    assert len(t_mat) >= 12
    assert "taxa_mortalidade_infantil_1000_nv" in t_mat.column_names

    t_pre = pq.read_table(p_pre)
    assert len(t_pre) >= 12
    assert "anos_potenciais_vida_perdidos_1000_hab" in t_pre.column_names

    t_gold = pq.read_table(p_gold)
    assert len(t_gold) >= 12
    assert "score_qualidade_vigilancia_obitos_sim" in t_gold.column_names
    assert "score_carga_mortalidade_evitavel" in t_gold.column_names
    assert "perfil_transicao_epidemiologica_mortalidade" in t_gold.column_names


def test_connector_methods_and_registry() -> None:
    """Testa métodos de consulta do conector e registro dinâmico por source_id."""
    dynamic_conn = get_connector("datasus_sim")
    assert isinstance(dynamic_conn, DataSUSSimConnector)

    gen = dynamic_conn.get_general_mortality("3550308")
    assert gen["id_municipio"] == "3550308"
    assert gen["total_obitos_registrados"] > 0

    cid10 = dynamic_conn.list_cid10_mortality_causes(
        "3550308", capitulo_cid10="Circulatorio"
    )
    assert len(cid10) == 1
    assert "Circulatorio" in cid10[0]["capitulo_cid10"]

    mat = dynamic_conn.get_maternal_infant_mortality("3550308")
    assert mat["id_municipio"] == "3550308"
    assert mat["taxa_mortalidade_infantil_1000_nv"] > 0

    pre = dynamic_conn.get_premature_dcnt_external_mortality("3550308")
    assert pre["id_municipio"] == "3550308"
    assert pre["anos_potenciais_vida_perdidos_1000_hab"] > 0

    features = dynamic_conn.get_sim_mortality_features("4106900")
    assert features["id_municipio"] == "4106900"
    assert 0.0 <= features["score_qualidade_vigilancia_obitos_sim"] <= 100.0
    assert 0.0 <= features["score_carga_mortalidade_evitavel"] <= 100.0

    ranking = dynamic_conn.list_top_sim_surveillance_municipalities(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_qualidade_vigilancia_obitos_sim"]
        >= ranking[-1]["score_qualidade_vigilancia_obitos_sim"]
    )


def test_sdk_parity() -> None:
    """Testa a paridade das chamadas DataSUS SIM no Python SDK BrazilData."""
    sdk = BrazilData()

    gen = sdk.get_sim_general_mortality("3550308")
    assert gen["id_municipio"] == "3550308"

    cid = sdk.list_sim_cid10_causes("3550308")
    assert len(cid) == 4

    mat = sdk.get_sim_maternal_infant_mortality("3550308")
    assert mat["id_municipio"] == "3550308"

    pre = sdk.get_sim_premature_dcnt_external("3550308")
    assert pre["id_municipio"] == "3550308"

    feat = sdk.get_sim_mortality_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_sim_surveillance_municipalities(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de sim general, cid10, maternal-infant, premature, features e ranking."""
    assert main(["sim", "general", "3550308", "--json"]) == 0
    gen_data = json.loads(capsys.readouterr().out)
    assert gen_data["id_municipio"] == "3550308"

    assert main(["sim", "cid10", "3550308", "--capitulo", "Circulatorio"]) == 0
    assert "Circulatorio" in capsys.readouterr().out

    assert main(["sim", "maternal-infant", "3550308"]) == 0
    assert "DataSUS SIM Mortalidade Materno-Infantil" in capsys.readouterr().out

    assert main(["sim", "premature", "3550308"]) == 0
    assert "DataSUS SIM Carga Prematura" in capsys.readouterr().out

    assert main(["sim", "features", "3550308"]) == 0
    assert "Score Vigilância SIM" in capsys.readouterr().out

    assert main(["sim", "ranking", "--limit", "3"]) == 0
    assert "Vigilância SIM" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para DataSUS SIM."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/health-public/sim/top-municipalities?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/health-public/sim/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_cid = client.get("/api/v1/health-public/sim/cid10/3550308?capitulo=Circulatorio")
    assert r_cid.status_code == 200
    assert len(r_cid.json()) == 1

    r_mat = client.get("/api/v1/health-public/sim/maternal-infant/3550308")
    assert r_mat.status_code == 200
    assert r_mat.json()["id_municipio"] == "3550308"

    r_pre = client.get("/api/v1/health-public/sim/premature/3550308")
    assert r_pre.status_code == 200
    assert r_pre.json()["id_municipio"] == "3550308"

    r_gen = client.get("/api/v1/health-public/sim/general/3550308")
    assert r_gen.status_code == 200
    assert r_gen.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/sim/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP DataSUS SIM e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_sim_general_mortality",
        "bdg_get_sim_maternal_infant_mortality",
        "bdg_get_sim_mortality_features",
        "bdg_list_top_sim_surveillance_municipalities",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_sim_general_mortality", {"id_municipio": "3550308"}),
        ("bdg_get_sim_maternal_infant_mortality", {"id_municipio": "3550308"}),
        ("bdg_get_sim_mortality_features", {"id_municipio": "3550308"}),
        ("bdg_list_top_sim_surveillance_municipalities", {"limit": 2}),
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
        assert "datasus_sim" in content
