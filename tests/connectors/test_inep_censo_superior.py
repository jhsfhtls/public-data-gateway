"""Tests for INEP Censo da Educação Superior Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
    INEPCensoSuperiorConnector,
    _classify_censo_superior_hub,
    _compute_censo_superior_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_censo_superior_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = INEPCensoSuperiorConnector()
    if not (conn.silver_dir / "censo_superior_instituicoes_ies.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "educacao"
        / "inep_censo_superior"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-INEP-CENSO-SUPERIOR-001"
    assert data["source_id"] == "inep_censo_superior"
    tables = data["schema"]["tables"]
    assert "censo_superior_instituicoes_ies" in tables
    assert "censo_superior_cursos_areas_cine" in tables
    assert "censo_superior_matriculas_ingressantes_concluintes" in tables
    assert "censo_superior_docentes_titulacao_regime" in tables
    assert "features_censo_superior_universidades_cursos_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e classificação de polo superior."""
    score_exc, score_cap = _compute_censo_superior_scores(
        igc_medio_continuo_municipio=4.18,
        proporcao_doutores_pct=64.5,
        proporcao_regime_tempo_integral_de_pct=66.0,
        taxa_conclusao_acumulada_tcg_pct=54.8,
        proporcao_matriculas_stem_ti_pct=28.4,
        taxa_desistencia_anual_tda_pct=13.8,
    )
    assert 85.0 <= score_exc <= 90.0
    assert 70.0 <= score_cap <= 75.0
    assert _classify_censo_superior_hub(score_exc) == "HUB_UNIVERSITARIO_PESQUISA_PONTA"
    assert _classify_censo_superior_hub(75.0) == "POLO_CONSOLIDADO_ENSINO_SUPERIOR"
    assert _classify_censo_superior_hub(64.0) == "POLO_REGIONAL_EXPANSAO_GRADUACAO"
    assert _classify_censo_superior_hub(49.0) == "REDE_EM_DESENVOLVIMENTO_ACESSO_SUPERIOR"


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/inep_censo_superior/."""
    conn = INEPCensoSuperiorConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_ies = conn.silver_dir / "censo_superior_instituicoes_ies.parquet"
    p_cur = conn.silver_dir / "censo_superior_cursos_areas_cine.parquet"
    p_mat = conn.silver_dir / "censo_superior_matriculas_ingressantes_concluintes.parquet"
    p_doc = conn.silver_dir / "censo_superior_docentes_titulacao_regime.parquet"
    p_gold = (
        conn.gold_dir
        / "features_censo_superior_universidades_cursos_municipios_brasil.parquet"
    )
    for path in (p_ies, p_cur, p_mat, p_doc, p_gold):
        assert path.exists(), f"Missing baseline parquet: {path}"

    assert pq.read_table(p_ies).num_rows >= 12
    assert pq.read_table(p_cur).num_rows >= 48
    assert pq.read_table(p_mat).num_rows >= 12
    assert pq.read_table(p_doc).num_rows >= 12
    gold_tbl = pq.read_table(p_gold)
    assert gold_tbl.num_rows >= 12
    assert "score_excelencia_academica_pesquisa_ies" in gold_tbl.column_names
    assert "score_formacao_capital_humano_superior" in gold_tbl.column_names
    assert "classificacao_polo_educacao_superior" in gold_tbl.column_names


def test_connector_registry_and_methods() -> None:
    """Testa registro global do conector e os 6 métodos analíticos."""
    conn = get_connector("inep_censo_superior")
    assert isinstance(conn, INEPCensoSuperiorConnector)
    assert conn.test_connection() is True

    ies = conn.get_institutions_summary("3550308")
    assert ies["id_municipio"] == "3550308"
    assert ies["cnpj_14"] == "63025530000104"
    assert ies["total_ies_ativas"] > 0

    courses_all = conn.list_cine_area_courses("3550308")
    assert len(courses_all) == 4
    courses_stem = conn.list_cine_area_courses("3550308", area_cine_rotulo="STEM_Engenharia_TI")
    assert len(courses_stem) == 1
    assert courses_stem[0]["area_cine_rotulo"] == "STEM_Engenharia_TI"

    enr = conn.get_enrollments_and_graduates("3550308")
    assert enr["id_municipio"] == "3550308"
    assert enr["total_matriculas_graduacao"] > 0

    fac = conn.get_faculty_profile("3550308")
    assert fac["id_municipio"] == "3550308"
    assert fac["proporcao_doutores_pct"] > 0

    feat = conn.get_censo_superior_features("3550308")
    assert feat["id_municipio"] == "3550308"
    assert feat["classificacao_polo_educacao_superior"] == "HUB_UNIVERSITARIO_PESQUISA_PONTA"

    ranks_all = conn.list_top_censo_superior_hubs(limit=5)
    assert len(ranks_all) == 5
    ranks_pr = conn.list_top_censo_superior_hubs(limit=5, sigla_uf="PR")
    assert len(ranks_pr) >= 1
    assert all(r["sigla_uf"] == "PR" for r in ranks_pr)


def test_sdk_parity() -> None:
    """Testa a paridade dos métodos no SDK Python (EducationFacade e BrazilData)."""
    sdk = BrazilData()

    ies = sdk.get_censo_superior_institutions("3550308")
    assert ies["id_municipio"] == "3550308"

    courses = sdk.list_censo_superior_cine_courses("3550308", area_cine_rotulo="STEM_Engenharia_TI")
    assert len(courses) == 1

    enr = sdk.get_censo_superior_enrollments_graduates("3550308")
    assert enr["id_municipio"] == "3550308"

    fac = sdk.get_censo_superior_faculty_profile("3550308")
    assert fac["id_municipio"] == "3550308"

    feat = sdk.get_censo_superior_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_censo_superior_hubs(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de censo-superior institutions, courses, enrollments, faculty, features e ranking."""
    assert main(["censo-superior", "institutions", "3550308", "--json"]) == 0
    ies_data = json.loads(capsys.readouterr().out)
    assert ies_data["id_municipio"] == "3550308"

    assert main(["censo-superior", "courses", "3550308", "--area", "STEM_Engenharia_TI"]) == 0
    assert "STEM_Engenharia_TI" in capsys.readouterr().out

    assert main(["censo-superior", "enrollments", "3550308"]) == 0
    assert "INEP Censo Superior Matrículas & Fluxo" in capsys.readouterr().out

    assert main(["censo-superior", "faculty", "3550308"]) == 0
    assert "INEP Censo Superior Corpo Docente" in capsys.readouterr().out

    assert main(["censo-superior", "features", "3550308"]) == 0
    assert "Score Excelência IES" in capsys.readouterr().out

    assert main(["censo-superior", "ranking", "--limit", "3"]) == 0
    assert "Polos Universitários" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para INEP Censo da Educação Superior."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/education/censo-superior/top-hubs?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/education/censo-superior/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_crs = client.get("/api/v1/education/censo-superior/courses/3550308?area=STEM_Engenharia_TI")
    assert r_crs.status_code == 200
    assert len(r_crs.json()) == 1

    r_enr = client.get("/api/v1/education/censo-superior/enrollments/3550308")
    assert r_enr.status_code == 200
    assert r_enr.json()["id_municipio"] == "3550308"

    r_fac = client.get("/api/v1/education/censo-superior/faculty/3550308")
    assert r_fac.status_code == 200
    assert r_fac.json()["id_municipio"] == "3550308"

    r_ies = client.get("/api/v1/education/censo-superior/institutions/3550308")
    assert r_ies.status_code == 200
    assert r_ies.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/censo-superior/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP INEP Censo Superior e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_censo_superior_institutions",
        "bdg_get_censo_superior_faculty",
        "bdg_get_censo_superior_features",
        "bdg_list_top_censo_superior_hubs",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_censo_superior_institutions", {"id_municipio": "3550308"}),
        ("bdg_get_censo_superior_faculty", {"id_municipio": "3550308"}),
        ("bdg_get_censo_superior_features", {"id_municipio": "3550308"}),
        ("bdg_list_top_censo_superior_hubs", {"limit": 2}),
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
        assert "inep_censo_superior" in content
