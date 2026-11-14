"""Tests for DataSUS SINASC Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
    DataSUSSinascConnector,
    _classify_sinasc_tier,
    _compute_sinasc_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_sinasc_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = DataSUSSinascConnector()
    if not (conn.silver_dir / "sinasc_nascidos_vivos_municipio.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "saude"
        / "datasus_sinasc"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-DATASUS-SINASC-001"
    assert data["source_id"] == "datasus_sinasc"
    tables = data["schema"]["tables"]
    assert "sinasc_nascidos_vivos_municipio" in tables
    assert "sinasc_assistencia_prenatal_parto" in tables
    assert "sinasc_perfil_materno_demografico" in tables
    assert "sinasc_condicoes_vitalidade_recem_nascido" in tables
    assert "features_sinasc_natalidade_saude_perinatal_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e classificação materno-perinatal."""
    score_pre, score_vit = _compute_sinasc_scores(
        cobertura_prenatal_7_mais_consultas_pct=86.4,
        inicio_prenatal_oportuno_1_trim_pct=88.2,
        proporcao_gravidez_adolescencia_pct=7.2,
        proporcao_escolaridade_materna_12_anos_mais_pct=42.6,
        proporcao_baixo_peso_nascer_pct=8.2,
        proporcao_prematuridade_pct=10.1,
        proporcao_apgar_5min_adequado_7_10_pct=98.8,
        proporcao_partos_hospitalares_pct=99.4,
    )
    assert 85.0 <= score_pre <= 100.0
    assert 75.0 <= score_vit <= 100.0
    assert (
        _classify_sinasc_tier(score_prenatal=score_pre, score_vitalidade=score_vit)
        == "REDE_PERINATAL_ALTA_COBERTURA_BAIXO_RISCO"
    )
    assert (
        _classify_sinasc_tier(score_prenatal=75.0, score_vitalidade=70.0)
        == "REDE_MATERNO_INFANTIL_CONSOLIDADA"
    )
    assert (
        _classify_sinasc_tier(score_prenatal=55.0, score_vitalidade=65.0)
        == "REDE_PERINATAL_EM_QUALIFICACAO_PRENATAL"
    )
    assert (
        _classify_sinasc_tier(score_prenatal=35.0, score_vitalidade=52.0)
        == "REDE_PRIORITARIA_VIGILANCIA_GESTACIONAL"
    )


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/datasus_sinasc/."""
    conn = DataSUSSinascConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_nv = conn.silver_dir / "sinasc_nascidos_vivos_municipio.parquet"
    p_pre = conn.silver_dir / "sinasc_assistencia_prenatal_parto.parquet"
    p_mat = conn.silver_dir / "sinasc_perfil_materno_demografico.parquet"
    p_vit = conn.silver_dir / "sinasc_condicoes_vitalidade_recem_nascido.parquet"
    p_gold = (
        conn.gold_dir
        / "features_sinasc_natalidade_saude_perinatal_municipios_brasil.parquet"
    )

    for path in (p_nv, p_pre, p_mat, p_vit, p_gold):
        assert path.exists(), f"Parquet ausente: {path}"

    t_nv = pq.read_table(p_nv)
    assert len(t_nv) >= 12
    assert "total_nascidos_vivos" in t_nv.column_names

    t_pre = pq.read_table(p_pre)
    assert len(t_pre) >= 48
    assert "faixa_assistencia_prenatal" in t_pre.column_names

    t_mat = pq.read_table(p_mat)
    assert len(t_mat) >= 12
    assert "idade_media_materna_anos" in t_mat.column_names

    t_vit = pq.read_table(p_vit)
    assert len(t_vit) >= 12
    assert "proporcao_baixo_peso_nascer_pct" in t_vit.column_names

    t_gold = pq.read_table(p_gold)
    assert len(t_gold) >= 12
    assert "score_qualidade_assistencia_prenatal_sinasc" in t_gold.column_names
    assert "score_vitalidade_protecao_perinatal" in t_gold.column_names
    assert "classificacao_rede_materno_perinatal" in t_gold.column_names


def test_connector_methods_and_registry() -> None:
    """Testa métodos de consulta do conector e registro dinâmico por source_id."""
    dynamic_conn = get_connector("datasus_sinasc")
    assert isinstance(dynamic_conn, DataSUSSinascConnector)

    births = dynamic_conn.get_live_births_summary("3550308")
    assert births["id_municipio"] == "3550308"
    assert births["total_nascidos_vivos"] > 0

    prenatal = dynamic_conn.list_prenatal_care_breakdown(
        "3550308", faixa_assistencia_prenatal="Adequado_Mais_7"
    )
    assert len(prenatal) == 1
    assert "Adequado_Mais_7" in prenatal[0]["faixa_assistencia_prenatal"]

    mat = dynamic_conn.get_maternal_demographic_profile("3550308")
    assert mat["id_municipio"] == "3550308"
    assert mat["idade_media_materna_anos"] > 20.0

    vit = dynamic_conn.get_newborn_vitality_conditions("3550308")
    assert vit["id_municipio"] == "3550308"
    assert vit["proporcao_apgar_5min_adequado_7_10_pct"] > 90.0

    features = dynamic_conn.get_sinasc_perinatal_features("4106900")
    assert features["id_municipio"] == "4106900"
    assert 0.0 <= features["score_qualidade_assistencia_prenatal_sinasc"] <= 100.0
    assert 0.0 <= features["score_vitalidade_protecao_perinatal"] <= 100.0

    ranking = dynamic_conn.list_top_sinasc_prenatal_municipalities(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_qualidade_assistencia_prenatal_sinasc"]
        >= ranking[-1]["score_qualidade_assistencia_prenatal_sinasc"]
    )


def test_sdk_parity() -> None:
    """Testa a paridade das chamadas DataSUS SINASC no Python SDK BrazilData."""
    sdk = BrazilData()

    births = sdk.get_sinasc_live_births("3550308")
    assert births["id_municipio"] == "3550308"

    prenatal = sdk.list_sinasc_prenatal_care("3550308")
    assert len(prenatal) == 4

    mat = sdk.get_sinasc_maternal_profile("3550308")
    assert mat["id_municipio"] == "3550308"

    vit = sdk.get_sinasc_newborn_vitality("3550308")
    assert vit["id_municipio"] == "3550308"

    feat = sdk.get_sinasc_perinatal_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_sinasc_prenatal_municipalities(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de sinasc births, prenatal, maternal, vitality, features e ranking."""
    assert main(["sinasc", "births", "3550308", "--json"]) == 0
    bir_data = json.loads(capsys.readouterr().out)
    assert bir_data["id_municipio"] == "3550308"

    assert main(["sinasc", "prenatal", "3550308", "--faixa", "Adequado_Mais_7"]) == 0
    assert "Adequado" in capsys.readouterr().out

    assert main(["sinasc", "maternal", "3550308"]) == 0
    assert "DataSUS SINASC Perfil Materno" in capsys.readouterr().out

    assert main(["sinasc", "vitality", "3550308"]) == 0
    assert "DataSUS SINASC Vitalidade Neonatal" in capsys.readouterr().out

    assert main(["sinasc", "features", "3550308"]) == 0
    assert "Score Pré-Natal" in capsys.readouterr().out

    assert main(["sinasc", "ranking", "--limit", "3"]) == 0
    assert "Materno-Perinatais" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para DataSUS SINASC."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/health-public/sinasc/top-municipalities?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/health-public/sinasc/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_pre = client.get("/api/v1/health-public/sinasc/prenatal/3550308?faixa=Adequado_Mais_7")
    assert r_pre.status_code == 200
    assert len(r_pre.json()) == 1

    r_mat = client.get("/api/v1/health-public/sinasc/maternal/3550308")
    assert r_mat.status_code == 200
    assert r_mat.json()["id_municipio"] == "3550308"

    r_vit = client.get("/api/v1/health-public/sinasc/vitality/3550308")
    assert r_vit.status_code == 200
    assert r_vit.json()["id_municipio"] == "3550308"

    r_bir = client.get("/api/v1/health-public/sinasc/births/3550308")
    assert r_bir.status_code == 200
    assert r_bir.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/sinasc/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP DataSUS SINASC e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_sinasc_live_births",
        "bdg_get_sinasc_newborn_vitality",
        "bdg_get_sinasc_perinatal_features",
        "bdg_list_top_sinasc_prenatal_municipalities",
    ):
        assert expected in tool_names

    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_sinasc_perinatal_features",
            "arguments": {"id_municipio": "4106900"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert res_call["result"]["isError"] is False
    text = res_call["result"]["content"][0]["text"]
    assert "<untrusted_gov_data source=\"datasus_sinasc\">" in text
    assert "4106900" in text

    req_top = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "bdg_list_top_sinasc_prenatal_municipalities",
            "arguments": {"limit": 3, "sigla_uf": "SP"},
        },
    }
    res_top = dispatcher.handle_request(req_top)
    assert res_top["result"]["isError"] is False
    assert "3550308" in res_top["result"]["content"][0]["text"]
