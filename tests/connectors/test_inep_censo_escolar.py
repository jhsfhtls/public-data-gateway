"""Tests for INEP Censo Escolar da Educação Básica Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
    INEPCensoEscolarConnector,
    _classify_censo_escolar_network,
    _compute_censo_escolar_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_censo_escolar_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = INEPCensoEscolarConnector()
    if not (conn.silver_dir / "censo_escolar_escolas_infraestrutura.parquet").exists():
        conn.extract_all()


def test_inep_censo_escolar_contract_metadata() -> None:
    """Verifica a integridade estrutural do contrato ODCS v3.1 de inep_censo_escolar."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "educacao"
        / "inep_censo_escolar"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-INEP-CENSO-ESCOLAR-001"
    assert data["source_id"] == "inep_censo_escolar"
    assert data["data_governance"]["primary_keys"] == ["id_municipio", "sigla_uf", "cep_8"]
    tables = data["schema"]["tables"]
    for table_name in (
        "censo_escolar_escolas_infraestrutura",
        "censo_escolar_matriculas_etapas",
        "censo_escolar_docentes_qualificacao",
        "censo_escolar_rendimento_fluxo",
        "features_censo_escolar_educacao_basica_municipios_brasil",
    ):
        assert table_name in tables


def test_inep_censo_escolar_scoring_and_classification() -> None:
    """Valida cálculo determinístico de scores (0-100) e classificação nos 4 tiers."""
    score_infra, score_fluxo = _compute_censo_escolar_scores(
        escolas_com_internet_banda_larga_pct=98.4,
        escolas_com_laboratorio_informatica_pct=83.0,
        escolas_com_laboratorio_ciencias_pct=62.5,
        escolas_com_biblioteca_pct=89.2,
        escolas_acessibilidade_pcd_pct=83.0,
        indicador_adequacao_formacao_docente_afd_pct=85.4,
        proporcao_tempo_integral_total_pct=36.2,
        taxa_aprovacao_media_pct=96.2,
        taxa_abandono_escolar_pct=0.7,
    )
    assert 90.0 <= score_infra <= 100.0
    assert 88.0 <= score_fluxo <= 100.0
    assert (
        _classify_censo_escolar_network(score_infra, score_fluxo)
        == "REDE_EDUCACIONAL_EXCELENCIA_INTEGRAL"
    )
    assert (
        _classify_censo_escolar_network(84.98, 79.59)
        == "POLO_EDUCACIONAL_INFRAESTRUTURA_AVANCADA"
    )
    assert (
        _classify_censo_escolar_network(68.0, 72.0)
        == "REDE_ENSINO_BASICO_CONSOLIDADA"
    )
    assert (
        _classify_censo_escolar_network(54.37, 52.64)
        == "REDE_PRIORITARIA_EXPANSAO_INFRAESTRUTURA"
    )


def test_inep_censo_escolar_baseline_parquets() -> None:
    """Valida a materialização dos 4 Parquets Silver e da Feature Store Gold."""
    conn = INEPCensoEscolarConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_esc = conn.silver_dir / "censo_escolar_escolas_infraestrutura.parquet"
    p_mat = conn.silver_dir / "censo_escolar_matriculas_etapas.parquet"
    p_doc = conn.silver_dir / "censo_escolar_docentes_qualificacao.parquet"
    p_ren = conn.silver_dir / "censo_escolar_rendimento_fluxo.parquet"
    p_gold = (
        conn.gold_dir
        / "features_censo_escolar_educacao_basica_municipios_brasil.parquet"
    )

    for path in (p_esc, p_mat, p_doc, p_ren, p_gold):
        assert path.exists(), f"Parquet ausente: {path}"

    assert len(pq.read_table(p_esc)) >= 12
    assert len(pq.read_table(p_mat)) >= 60
    assert len(pq.read_table(p_doc)) >= 12
    assert len(pq.read_table(p_ren)) >= 12

    t_gold = pq.read_table(p_gold)
    assert len(t_gold) >= 12
    assert "score_infraestrutura_escolar_inep" in t_gold.column_names
    assert "score_qualidade_fluxo_educacional" in t_gold.column_names
    assert "classificacao_rede_educacional_basica" in t_gold.column_names


def test_inep_censo_escolar_connector_methods_and_registry() -> None:
    """Valida get_connector dinâmico e métodos de consulta do conector Censo Escolar."""
    conn = get_connector("inep_censo_escolar")
    assert isinstance(conn, INEPCensoEscolarConnector)

    sch = conn.get_school_infrastructure("4106900")
    assert sch["id_municipio"] == "4106900"
    assert sch["cep_8"] == "80010010"
    assert sch["total_escolas_ativas"] > 0

    enr = conn.list_enrollments_by_stage("3550308", etapa_ensino="Ensino_Medio")
    assert len(enr) == 1
    assert enr[0]["etapa_ensino"] == "Ensino_Medio"

    tch = conn.get_teacher_qualifications("3550308")
    assert tch["total_docentes_exercicio"] > 0
    assert tch["indicador_adequacao_formacao_docente_afd_pct"] > 0

    flw = conn.get_yield_flow_indicators("3550308")
    assert flw["taxa_aprovacao_media_pct"] > 80.0

    feat = conn.get_censo_escolar_features("4106900")
    assert feat["classificacao_rede_educacional_basica"] == "REDE_EDUCACIONAL_EXCELENCIA_INTEGRAL"

    ranks = conn.list_top_censo_escolar_municipalities(limit=5)
    assert len(ranks) == 5
    assert (
        ranks[0]["score_infraestrutura_escolar_inep"]
        >= ranks[-1]["score_infraestrutura_escolar_inep"]
    )


def test_inep_censo_escolar_sdk_integration() -> None:
    """Valida paridade completa no Python SDK BrazilData.education e atalhos diretos."""
    sdk = BrazilData()

    assert sdk.get_censo_escolar_infrastructure("3550308")["id_municipio"] == "3550308"
    assert len(sdk.list_censo_escolar_enrollments("3550308")) == 5
    assert sdk.get_censo_escolar_teachers("3550308")["id_municipio"] == "3550308"
    assert sdk.get_censo_escolar_yield_flow("3550308")["id_municipio"] == "3550308"
    assert sdk.get_censo_escolar_features("4106900")["score_infraestrutura_escolar_inep"] >= 85.0
    assert len(sdk.list_top_censo_escolar_municipalities(limit=4)) == 4


def test_inep_censo_escolar_cli_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Valida subcomandos CLI bdg censo-escolar com saída JSON e legível."""
    assert main(["censo-escolar", "schools", "3550308", "--json"]) == 0
    schools_data = json.loads(capsys.readouterr().out)
    assert schools_data["id_municipio"] == "3550308"

    assert main(["censo-escolar", "enrollments", "3550308", "--etapa", "Ensino_Medio", "--json"]) == 0
    enr_data = json.loads(capsys.readouterr().out)
    assert len(enr_data) == 1

    assert main(["censo-escolar", "teachers", "3550308"]) == 0
    assert "INEP Censo Escolar Docentes" in capsys.readouterr().out

    assert main(["censo-escolar", "flow", "3550308"]) == 0
    assert "INEP Censo Escolar Rendimento" in capsys.readouterr().out

    assert main(["censo-escolar", "features", "4106900"]) == 0
    assert "Score Infraestrutura" in capsys.readouterr().out

    assert main(["censo-escolar", "ranking", "--limit", "3"]) == 0
    assert "Redes Municipais de Educação Básica" in capsys.readouterr().out


def test_inep_censo_escolar_rest_api() -> None:
    """Valida paridade dos endpoints FastAPI REST para INEP Censo Escolar."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/education/censo-escolar/top-municipalities?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/education/censo-escolar/features/4106900")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "4106900"

    r_enr = client.get("/api/v1/education/censo-escolar/enrollments/3550308?etapa=Ensino_Medio")
    assert r_enr.status_code == 200
    assert len(r_enr.json()) == 1

    r_tch = client.get("/api/v1/education/censo-escolar/teachers/3550308")
    assert r_tch.status_code == 200
    assert r_tch.json()["id_municipio"] == "3550308"

    r_flw = client.get("/api/v1/education/censo-escolar/flow/3550308")
    assert r_flw.status_code == 200
    assert r_flw.json()["id_municipio"] == "3550308"

    r_sch = client.get("/api/v1/education/censo-escolar/schools/3550308")
    assert r_sch.status_code == 200
    assert r_sch.json()["id_municipio"] == "3550308"

    r_alias = client.get("/api/v1/censo-escolar/3550308")
    assert r_alias.status_code == 200
    assert r_alias.json()["id_municipio"] == "3550308"


def test_inep_censo_escolar_mcp_tools() -> None:
    """Valida ferramentas MCP de INEP Censo Escolar e envelope untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_censo_escolar_schools",
        "bdg_get_censo_escolar_teachers",
        "bdg_get_censo_escolar_features",
        "bdg_list_top_censo_escolar_municipalities",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_censo_escolar_schools", {"id_municipio": "3550308"}),
        ("bdg_get_censo_escolar_teachers", {"id_municipio": "3550308"}),
        ("bdg_get_censo_escolar_features", {"id_municipio": "4106900"}),
        ("bdg_list_top_censo_escolar_municipalities", {"limit": 2}),
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
        assert "inep_censo_escolar" in content
