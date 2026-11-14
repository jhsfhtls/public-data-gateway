"""Tests for INEP ENEM Performance, School Networks, Essay Competencies & Equity Connector."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.educacao.inep_enem.connector import (
    INEPEnemConnector,
    _classify_enem_tier,
    _compute_enem_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_enem_baseline() -> None:
    """Garante que os 5 Parquets de baseline existam em ambiente CI limpo."""
    conn = INEPEnemConnector()
    required = [
        conn.silver_dir / "enem_desempenho_municipal_areas.parquet",
        conn.silver_dir / "enem_desempenho_rede_ensino.parquet",
        conn.silver_dir / "enem_redacao_competencias_municipio.parquet",
        conn.silver_dir / "enem_equidade_socioeconomica_acesso.parquet",
        conn.gold_dir / "features_enem_desempenho_equidade_municipios_brasil.parquet",
    ]
    if not all(path.exists() for path in required):
        conn.materialize_all()


def test_inep_enem_contract_validity() -> None:
    """Verifica a integridade do contrato ODCS v3.1 de inep_enem."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "educacao"
        / "inep_enem"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-INEP-ENEM-001"
    assert data["source_id"] == "inep_enem"
    assert data["data_governance"]["lgpd_tier"] == "ANONYMIZED_MICRODATA"
    assert data["data_governance"]["primary_keys"] == ["id_municipio", "sigla_uf"]

    tables = data["schema"]["tables"]
    expected_tables = (
        "enem_desempenho_municipal_areas",
        "enem_desempenho_rede_ensino",
        "enem_redacao_competencias_municipio",
        "enem_equidade_socioeconomica_acesso",
        "features_enem_desempenho_equidade_municipios_brasil",
    )
    assert len(tables) == 5
    for table_name in expected_tables:
        assert table_name in tables
        col_names = {col["name"] for col in tables[table_name]["columns"]}
        assert {"id_municipio", "nome_municipio", "sigla_uf", "reference_date"}.issubset(
            col_names
        )


def test_inep_enem_score_and_tier_classification() -> None:
    """Valida cálculo determinístico de scores (0-100) e classificação nos 4 tiers."""
    score_desemp, score_eq = _compute_enem_scores(
        nota_media_objetivas_tri_geral=577.0,
        nota_media_redacao_enem=702.0,
        proporcao_redacao_900_mais_pct=13.80,
        taxa_presenca_efetiva_2dias_pct=78.0,
        aptos_sisu_prouni_fies_nota_450_pct=85.6,
        razao_equidade_publica_privada_enem=0.84,
    )
    assert score_desemp == 83.63
    assert score_eq == 82.76
    assert (
        _classify_enem_tier(score_desempenho=score_desemp, score_equidade=score_eq)
        == "POLO_EXCELENCIA_COGNITIVA_E_REDACAO_ENEM"
    )
    assert (
        _classify_enem_tier(score_desempenho=76.5, score_equidade=78.0)
        == "REDE_ALTO_DESEMPENHO_ACESSO_SISU_PROUNI"
    )
    assert (
        _classify_enem_tier(score_desempenho=67.5, score_equidade=72.0)
        == "DESEMPENHO_INTERMEDIARIO_EM_CONSOLIDACAO"
    )
    assert (
        _classify_enem_tier(score_desempenho=58.0, score_equidade=68.0)
        == "TERRITORIO_PRIORITARIO_EQUIDADE_ENSINO_MEDIO"
    )


def test_inep_enem_baseline_parquets() -> None:
    """Valida os 5 Parquets Silver/Gold e a distribuição nos 4 tiers do ENEM."""
    conn = INEPEnemConnector()
    specs = [
        (conn.silver_dir / "enem_desempenho_municipal_areas.parquet", 12, 16),
        (conn.silver_dir / "enem_desempenho_rede_ensino.parquet", 48, 14),
        (conn.silver_dir / "enem_redacao_competencias_municipio.parquet", 12, 15),
        (conn.silver_dir / "enem_equidade_socioeconomica_acesso.parquet", 12, 15),
        (
            conn.gold_dir / "features_enem_desempenho_equidade_municipios_brasil.parquet",
            12,
            23,
        ),
    ]
    for path, exp_rows, exp_cols in specs:
        assert path.exists(), f"Parquet ausente: {path}"
        table = pq.read_table(path)
        assert table.num_rows == exp_rows
        assert table.num_columns == exp_cols

    gold_table = pq.read_table(specs[-1][0])
    tier_counts = Counter(gold_table["classificacao_desempenho_enem_municipal"].to_pylist())
    assert tier_counts == {
        "POLO_EXCELENCIA_COGNITIVA_E_REDACAO_ENEM": 3,
        "REDE_ALTO_DESEMPENHO_ACESSO_SISU_PROUNI": 3,
        "DESEMPENHO_INTERMEDIARIO_EM_CONSOLIDACAO": 3,
        "TERRITORIO_PRIORITARIO_EQUIDADE_ENSINO_MEDIO": 3,
    }


def test_inep_enem_connector_methods_and_resolution() -> None:
    """Valida registro dinâmico, extract_table, normalize, resoluções e erros."""
    conn = get_connector("inep_enem")
    assert isinstance(conn, INEPEnemConnector)

    assert len(conn.extract_table("enem_desempenho_rede_ensino")) == 48
    assert (
        len(
            conn.extract_table(
                "features_enem_desempenho_equidade_municipios_brasil"
            )
        )
        == 12
    )

    normalized = conn.normalize(
        [
            {
                "id_municipio": "4106900",
                "inscritos_confirmados_enem_ano": 68400,
                "participantes_presentes_2dias_ano": 53900,
                "nota_media_linguagens_codigos_lc": 568.0,
                "nota_media_ciencias_humanas_ch": 576.0,
                "nota_media_ciencias_natureza_cn": 574.0,
                "nota_media_matematica_mt": 612.0,
            }
        ]
    )
    assert normalized.num_rows == 1
    assert normalized.to_pylist()[0]["id_municipio"] == "4106902"

    # IBGE-7, IBGE-6, alias, CEP-8, nome oficial e fallback determinístico
    assert conn.get_enem_municipal_area_scores("3550308")["id_municipio"] == "3550308"
    assert conn.get_enem_essay_competencies("410690")["id_municipio"] == "4106902"
    assert conn.get_enem_socioeconomic_equity("4106900")["id_municipio"] == "4106902"
    assert conn.get_enem_performance_equity_features("01001-000")["id_municipio"] == "3550308"
    assert conn.get_enem_municipal_area_scores("São Paulo")["id_municipio"] == "3550308"

    fallback = conn.get_enem_performance_equity_features("Campinas")
    assert fallback["id_municipio"] == "3509502"
    assert fallback["score_desempenho_cognitivo_enem"] == 61.42
    assert fallback["score_equidade_acesso_ensino_superior_enem"] == 73.77

    redes_all = conn.list_enem_scores_by_school_network("3550308")
    assert len(redes_all) == 4
    redes_priv = conn.list_enem_scores_by_school_network(
        "3550308", dependencia_rede="Privada"
    )
    assert len(redes_priv) == 1
    assert "Privada" in redes_priv[0]["categoria_escola_enem"]

    ranks = conn.list_top_enem_municipalities(limit=5, sigla_uf="SP")
    assert len(ranks) == 1
    assert ranks[0]["id_municipio"] == "3550308"

    with pytest.raises(ValueError):
        conn.get_enem_municipal_area_scores("")
    with pytest.raises(ValueError):
        conn.get_enem_performance_equity_features("99999999")
    with pytest.raises(ValueError):
        conn.list_top_enem_municipalities(limit=0)
    with pytest.raises(ValueError):
        conn.extract_table("tabela_inexistente")


def test_sdk_education_facade_and_brazildata_parity() -> None:
    """Valida paridade completa dos 6 métodos em BrazilData.education e BrazilData."""
    bdg = BrazilData()
    conn = INEPEnemConnector()

    assert (
        bdg.education.get_enem_municipal_area_scores("3550308")
        == conn.get_enem_municipal_area_scores("3550308")
    )
    assert (
        bdg.get_enem_municipal_area_scores("3550308")
        == bdg.education.get_enem_municipal_area_scores("3550308")
    )
    assert (
        bdg.list_enem_scores_by_school_network("3550308", dependencia_rede="Privada")
        == bdg.education.list_enem_scores_by_school_network(
            "3550308", dependencia_rede="Privada"
        )
    )
    assert (
        bdg.get_enem_essay_competencies("4106900")
        == bdg.education.get_enem_essay_competencies("4106900")
    )
    assert (
        bdg.get_enem_socioeconomic_equity("3550308")
        == bdg.education.get_enem_socioeconomic_equity("3550308")
    )
    assert (
        bdg.get_enem_performance_equity_features("4106900")
        == bdg.education.get_enem_performance_equity_features("4106900")
    )
    ranks = bdg.list_top_enem_municipalities(limit=4)
    assert len(ranks) == 4
    assert ranks == bdg.education.list_top_enem_municipalities(limit=4)


def test_cli_enem_subcommands(capsys: pytest.CaptureFixture[str]) -> None:
    """Valida subcomandos CLI bdg enem (JSON, texto e erro)."""
    assert main(["enem", "scores", "3550308", "--json"]) == 0
    scores_json = json.loads(capsys.readouterr().out)
    assert scores_json["id_municipio"] == "3550308"

    assert main(["enem", "networks", "3550308", "--rede", "Privada", "--json"]) == 0
    networks_json = json.loads(capsys.readouterr().out)
    assert len(networks_json) == 1

    assert main(["enem", "essay", "4106900"]) == 0
    assert "INEP ENEM Redação & Competências" in capsys.readouterr().out

    assert main(["enem", "equity", "3550308"]) == 0
    assert "INEP ENEM Equidade QSE & Acesso" in capsys.readouterr().out

    assert main(["enem", "features", "01001000"]) == 0
    assert "Score Cognitivo" in capsys.readouterr().out

    assert main(["enem", "ranking", "--limit", "3"]) == 0
    assert "Desempenho & Equidade ENEM" in capsys.readouterr().out

    assert main(["enem", "scores", "99999999"]) == 1
    _ = capsys.readouterr()


def test_fastapi_enem_routes() -> None:
    """Valida endpoints FastAPI REST de inep_enem e tratamento de 400."""
    client = TestClient(create_app())

    r_top = client.get("/api/v1/education/enem/top-municipalities?limit=3&uf=SP")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 1
    assert r_top.json()[0]["id_municipio"] == "3550308"

    r_feat = client.get("/api/v1/education/enem/features/410690")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "4106902"

    r_net = client.get("/api/v1/education/enem/networks/3550308?rede=Privada")
    assert r_net.status_code == 200
    assert len(r_net.json()) == 1

    r_essay = client.get("/api/v1/education/enem/essay/3550308")
    assert r_essay.status_code == 200
    assert r_essay.json()["id_municipio"] == "3550308"

    r_eq = client.get("/api/v1/education/enem/equity/3550308")
    assert r_eq.status_code == 200
    assert r_eq.json()["id_municipio"] == "3550308"

    r_scores = client.get("/api/v1/education/enem/scores/3550308")
    assert r_scores.status_code == 200
    assert r_scores.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/enem/01001000")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"

    r_bad = client.get("/api/v1/education/enem/features/99999999")
    assert r_bad.status_code == 400


def test_mcp_enem_tools_and_error_handling() -> None:
    """Valida ferramentas MCP INEP ENEM, wrapper de segurança e erros."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_enem_municipal_area_scores",
        "bdg_get_enem_essay_competencies",
        "bdg_get_enem_performance_equity_features",
        "bdg_list_top_enem_municipalities",
    ):
        assert expected in tool_names

    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_enem_performance_equity_features",
            "arguments": {"id_municipio": "410690"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert res_call["result"]["isError"] is False
    text = res_call["result"]["content"][0]["text"]
    assert '<untrusted_gov_data source="inep_enem">' in text
    assert "4106902" in text

    req_scores = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_enem_municipal_area_scores",
            "arguments": {"id_municipio": "01001000"},
        },
    }
    res_scores = dispatcher.handle_request(req_scores)
    assert res_scores["result"]["isError"] is False
    assert "3550308" in res_scores["result"]["content"][0]["text"]

    req_top = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "bdg_list_top_enem_municipalities",
            "arguments": {"limit": 3, "sigla_uf": "SP"},
        },
    }
    res_top = dispatcher.handle_request(req_top)
    assert res_top["result"]["isError"] is False
    assert "3550308" in res_top["result"]["content"][0]["text"]

    req_err = {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_enem_essay_competencies",
            "arguments": {"id_municipio": ""},
        },
    }
    res_err = dispatcher.handle_request(req_err)
    assert res_err["result"]["isError"] is True
