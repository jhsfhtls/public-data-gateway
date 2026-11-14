"""Tests for INEP IDEB Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.educacao.inep_ideb.connector import (
    INEPIdebConnector,
    _classify_ideb_tier,
    _compute_ideb_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_ideb_baseline() -> None:
    """Garante que os Parquets de baseline IDEB existam em ambiente CI limpo."""
    conn = INEPIdebConnector()
    if not (conn.silver_dir / "ideb_resultados_municipios.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 INEP IDEB está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "educacao"
        / "inep_ideb"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-INEP-IDEB-001"
    assert data["source_id"] == "inep_ideb"
    tables = data["schema"]["tables"]
    assert "ideb_resultados_municipios" in tables
    assert "ideb_etapas_redes_ensino" in tables
    assert "ideb_proficiencia_saeb_aprendizagem" in tables
    assert "ideb_metas_evolucao_bienal" in tables
    assert "features_ideb_qualidade_educacional_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida cálculo dos scores normalizados (0-100) e classificação dos municípios no IDEB."""
    score_des, score_prof = _compute_ideb_scores(
        ideb_anos_iniciais_rede_publica=6.6,
        ideb_anos_finais_rede_publica=5.6,
        ideb_ensino_medio_rede_publica=4.9,
        proporcao_escolas_cumpriram_meta_pct=81.5,
        indicador_rendimento_fluxo_medio_p=0.97,
        aprendizado_adequado_anos_iniciais_pct=72.5,
        aprendizado_adequado_anos_finais_pct=53.0,
        equidade_aprendizagem_inse_ratio=0.89,
        taxa_participacao_saeb_pct=95.8,
    )
    assert 80.0 <= score_des <= 100.0
    assert 80.0 <= score_prof <= 100.0
    assert (
        _classify_ideb_tier(score_desempenho=score_des, score_proficiencia=score_prof)
        == "REDE_EXCELENCIA_APRENDIZAGEM_IDEB"
    )
    assert (
        _classify_ideb_tier(score_desempenho=68.0, score_proficiencia=72.0)
        == "DESEMPENHO_EDUCACIONAL_CONSOLIDADO_ACIMA_META"
    )
    assert (
        _classify_ideb_tier(score_desempenho=50.0, score_proficiencia=60.0)
        == "TRANSICAO_EDUCACIONAL_RECUPERACAO_FLUXO"
    )
    assert (
        _classify_ideb_tier(score_desempenho=35.0, score_proficiencia=40.0)
        == "TERRITORIO_PRIORITARIO_RECOMPOSICAO_APRENDIZAGEM"
    )


def test_baseline_parquets_exist() -> None:
    """Valida materialização física e paridade de colunas dos Parquets IDEB."""
    conn = INEPIdebConnector()
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "educacao"
        / "inep_ideb"
        / "contract.yaml"
    )
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))

    paths = {
        "ideb_resultados_municipios": conn.silver_dir
        / "ideb_resultados_municipios.parquet",
        "ideb_etapas_redes_ensino": conn.silver_dir
        / "ideb_etapas_redes_ensino.parquet",
        "ideb_proficiencia_saeb_aprendizagem": conn.silver_dir
        / "ideb_proficiencia_saeb_aprendizagem.parquet",
        "ideb_metas_evolucao_bienal": conn.silver_dir
        / "ideb_metas_evolucao_bienal.parquet",
        "features_ideb_qualidade_educacional_municipios_brasil": conn.gold_dir
        / "features_ideb_qualidade_educacional_municipios_brasil.parquet",
    }

    for tbl_name, path in paths.items():
        assert path.exists(), f"Parquet ausente: {path}"
        table = pq.read_table(path)
        expected_cols = [c["name"] for c in contract["schema"]["tables"][tbl_name]["columns"]]
        assert table.column_names == expected_cols
        min_rows = contract["schema"]["tables"][tbl_name]["min_expected_rows"]
        assert table.num_rows >= min_rows


def test_connector_methods_and_cep_alias_resolution() -> None:
    """Testa registro dinâmico, métodos IDEB e resolução por IBGE, alias, CEP-8 e nome."""
    dynamic_conn = get_connector("inep_ideb")
    assert isinstance(dynamic_conn, INEPIdebConnector)

    res_ibge = dynamic_conn.get_ideb_municipal_results("3550308")
    assert res_ibge["id_municipio"] == "3550308"
    assert res_ibge["cep_8"] == "01001000"
    assert res_ibge["ideb_anos_iniciais_rede_publica"] > 5.0

    res_cep = dynamic_conn.get_ideb_municipal_results("01001-000")
    assert res_cep["id_municipio"] == "3550308"

    res_alias = dynamic_conn.get_ideb_municipal_results("4106900")
    assert res_alias["id_municipio"] == "4106902"

    res_name = dynamic_conn.get_ideb_municipal_results("São Paulo")
    assert res_name["id_municipio"] == "3550308"

    stages = dynamic_conn.list_ideb_stage_network_scores(
        "4106902", etapa_ciclo="Anos_Iniciais"
    )
    assert len(stages) == 1
    assert stages[0]["etapa_ciclo_ideb"] == "Anos_Iniciais_EF_Rede_Publica"

    saeb = dynamic_conn.get_ideb_saeb_proficiency("4106902")
    assert saeb["id_municipio"] == "4106902"
    assert saeb["proficiencia_saeb_mat_9ano"] > 250.0

    evol = dynamic_conn.get_ideb_biennial_goals_evolution("4106902")
    assert evol["id_municipio"] == "4106902"
    assert evol["equidade_aprendizagem_inse_ratio"] > 0.8

    features = dynamic_conn.get_ideb_education_quality_features("4106902")
    assert features["id_municipio"] == "4106902"
    assert features["classificacao_qualidade_ideb_municipio"] == "REDE_EXCELENCIA_APRENDIZAGEM_IDEB"
    assert 0.0 <= features["score_desempenho_ideb_municipal"] <= 100.0
    assert 0.0 <= features["score_proficiencia_equidade_saeb"] <= 100.0

    ranking = dynamic_conn.list_top_ideb_municipalities(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_desempenho_ideb_municipal"]
        >= ranking[-1]["score_desempenho_ideb_municipal"]
    )


def test_sdk_parity() -> None:
    """Testa paridade das chamadas INEP IDEB na fachada EducationFacade e BrazilData."""
    sdk = BrazilData()

    res = sdk.get_ideb_municipal_results("3550308")
    assert res["id_municipio"] == "3550308"
    assert sdk.education.get_ideb_municipal_results("3550308")["id_municipio"] == "3550308"

    stages = sdk.list_ideb_stage_network_scores("3550308")
    assert len(stages) == 4

    saeb = sdk.get_ideb_saeb_proficiency("3550308")
    assert saeb["id_municipio"] == "3550308"

    evol = sdk.get_ideb_biennial_goals_evolution("3550308")
    assert evol["id_municipio"] == "3550308"

    feat = sdk.get_ideb_education_quality_features("4106902")
    assert feat["id_municipio"] == "4106902"

    ranks = sdk.list_top_ideb_municipalities(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os 6 subcomandos CLI de ideb."""
    assert main(["ideb", "results", "3550308", "--json"]) == 0
    res_data = json.loads(capsys.readouterr().out)
    assert res_data["id_municipio"] == "3550308"

    assert main(["ideb", "stages", "4106902", "--etapa", "Anos_Iniciais"]) == 0
    assert "Anos_Iniciais_EF_Rede_Publica" in capsys.readouterr().out

    assert main(["ideb", "saeb", "3550308"]) == 0
    assert "INEP IDEB/SAEB Proficiências" in capsys.readouterr().out

    assert main(["ideb", "evolution", "3550308"]) == 0
    assert "INEP IDEB Evolução & Metas" in capsys.readouterr().out

    assert main(["ideb", "features", "4106902"]) == 0
    assert "Score Desempenho IDEB" in capsys.readouterr().out

    assert main(["ideb", "ranking", "--limit", "3"]) == 0
    assert "Municípios no IDEB & Equidade SAEB" in capsys.readouterr().out

    assert main(["ideb", "results", "99999999"]) == 1
    _ = capsys.readouterr()


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para INEP IDEB."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/education/ideb/top-municipalities?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/education/ideb/features/4106902")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "4106902"

    r_stg = client.get("/api/v1/education/ideb/stages/4106902?etapa=Anos_Iniciais")
    assert r_stg.status_code == 200
    assert len(r_stg.json()) == 1

    r_saeb = client.get("/api/v1/education/ideb/saeb/3550308")
    assert r_saeb.status_code == 200
    assert r_saeb.json()["id_municipio"] == "3550308"

    r_ev = client.get("/api/v1/education/ideb/evolution/3550308")
    assert r_ev.status_code == 200
    assert r_ev.json()["id_municipio"] == "3550308"

    r_res = client.get("/api/v1/education/ideb/results/3550308")
    assert r_res.status_code == 200
    assert r_res.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/ideb/01001000")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"

    r_bad = client.get("/api/v1/education/ideb/features/99999999")
    assert r_bad.status_code == 400


def test_mcp_parity_and_error_handling() -> None:
    """Testa ferramentas MCP INEP IDEB, wrap untrusted_gov_data e validação de erros."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_ideb_municipal_results",
        "bdg_get_ideb_saeb_proficiency",
        "bdg_get_ideb_education_quality_features",
        "bdg_list_top_ideb_municipalities",
    ):
        assert expected in tool_names

    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_ideb_education_quality_features",
            "arguments": {"id_municipio": "4106902"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert res_call["result"]["isError"] is False
    text = res_call["result"]["content"][0]["text"]
    assert '<untrusted_gov_data source="inep_ideb">' in text
    assert "4106902" in text

    req_saeb = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_ideb_saeb_proficiency",
            "arguments": {"id_municipio": "01001000"},
        },
    }
    res_saeb = dispatcher.handle_request(req_saeb)
    assert res_saeb["result"]["isError"] is False
    assert "3550308" in res_saeb["result"]["content"][0]["text"]

    req_top = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "bdg_list_top_ideb_municipalities",
            "arguments": {"limit": 3, "sigla_uf": "PR"},
        },
    }
    res_top = dispatcher.handle_request(req_top)
    assert res_top["result"]["isError"] is False
    assert "4106902" in res_top["result"]["content"][0]["text"]

    req_err = {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_ideb_municipal_results",
            "arguments": {"id_municipio": ""},
        },
    }
    res_err = dispatcher.handle_request(req_err)
    assert res_err["result"]["isError"] is True

    conn = INEPIdebConnector()
    with pytest.raises(ValueError):
        conn.get_ideb_municipal_results("")
    with pytest.raises(ValueError):
        conn.list_top_ideb_municipalities(limit=0)
    with pytest.raises(ValueError):
        conn.get_ideb_education_quality_features("99999999")
