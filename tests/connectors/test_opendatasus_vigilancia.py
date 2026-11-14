"""Tests for OpenDataSUS Vigilância Epidemiológica, SRAG, SINAN, SI-PNI & CIEVS Connector."""

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
from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
    OpenDataSUSVigilanciaConnector,
    _classify_vigilancia_tier,
    _compute_vigilancia_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_vigilancia_baseline() -> None:
    """Garante que os 5 Parquets de baseline existam em ambiente CI limpo."""
    conn = OpenDataSUSVigilanciaConnector()
    required = [
        conn.silver_dir / "vigilancia_sindromes_respiratorias_srag_municipio.parquet",
        conn.silver_dir / "vigilancia_agravos_notificacao_sinan.parquet",
        conn.silver_dir / "vigilancia_cobertura_vacinal_sipni_municipio.parquet",
        conn.silver_dir / "vigilancia_capacidade_resposta_cievs_lacen.parquet",
        conn.gold_dir
        / "features_opendatasus_vigilancia_epidemiologica_municipios_brasil.parquet",
    ]
    if not all(path.exists() for path in required):
        conn.materialize_all()


def test_opendatasus_vigilancia_contract_validity() -> None:
    """Verifica a integridade do contrato ODCS v3.1 de opendatasus_vigilancia."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "saude"
        / "opendatasus_vigilancia"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-OPENDATASUS-VIGILANCIA-001"
    assert data["source_id"] == "opendatasus_vigilancia"
    assert data["data_governance"]["lgpd_tier"] == "ANONYMIZED_MICRODATA"
    assert data["data_governance"]["primary_keys"] == ["id_municipio", "sigla_uf"]

    tables = data["schema"]["tables"]
    expected_tables = (
        "vigilancia_sindromes_respiratorias_srag_municipio",
        "vigilancia_agravos_notificacao_sinan",
        "vigilancia_cobertura_vacinal_sipni_municipio",
        "vigilancia_capacidade_resposta_cievs_lacen",
        "features_opendatasus_vigilancia_epidemiologica_municipios_brasil",
    )
    assert len(tables) == 5
    for table_name in expected_tables:
        assert table_name in tables
        col_names = {col["name"] for col in tables[table_name]["columns"]}
        assert {"id_municipio", "nome_municipio", "sigla_uf", "reference_date"}.issubset(
            col_names
        )


def test_opendatasus_vigilancia_score_and_tier_classification() -> None:
    """Valida cálculo determinístico de scores (0-100) e classificação nos 4 tiers."""
    score_prontidao, score_pni = _compute_vigilancia_scores(
        confirmacao_laboratorial_rt_pcr_srag_pct=94.0,
        proporcao_encerramento_oportuno_sinan_pct=93.2,
        notificacoes_imediatas_24h_cievs_pct=95.2,
        integracao_rnds_esus_sinan_pct=96.0,
        cobertura_pentavalente_d3_pct=91.4,
        cobertura_triplice_viral_d1_pct=93.6,
        homogeneidade_cobertura_vacinal_pni_pct=88.0,
    )
    assert 90.0 <= score_prontidao <= 100.0
    assert 86.0 <= score_pni <= 100.0
    assert (
        _classify_vigilancia_tier(score_prontidao=score_prontidao, score_pni=score_pni)
        == "POLO_SENTINELA_ALTA_COBERTURA_IMUNIZACAO"
    )
    assert (
        _classify_vigilancia_tier(score_prontidao=84.5, score_pni=81.2)
        == "REDE_VIGILANCIA_EPIDEMIOLOGICA_CONSOLIDADA"
    )
    assert (
        _classify_vigilancia_tier(score_prontidao=78.0, score_pni=72.0)
        == "TERRITORIO_PRESSAO_ENDEMICA_EM_MONITORAMENTO"
    )
    assert (
        _classify_vigilancia_tier(score_prontidao=71.0, score_pni=68.0)
        == "ZONA_PRIORITARIA_CONTROLE_ARBOVIROSES_E_PNI"
    )


def test_opendatasus_vigilancia_baseline_parquets() -> None:
    """Valida os 5 Parquets Silver/Gold e a distribuição nos 4 tiers epidemiológicos."""
    conn = OpenDataSUSVigilanciaConnector()
    specs = [
        (
            conn.silver_dir / "vigilancia_sindromes_respiratorias_srag_municipio.parquet",
            12,
            16,
        ),
        (conn.silver_dir / "vigilancia_agravos_notificacao_sinan.parquet", 48, 14),
        (conn.silver_dir / "vigilancia_cobertura_vacinal_sipni_municipio.parquet", 12, 15),
        (conn.silver_dir / "vigilancia_capacidade_resposta_cievs_lacen.parquet", 12, 15),
        (
            conn.gold_dir
            / "features_opendatasus_vigilancia_epidemiologica_municipios_brasil.parquet",
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
    tier_counts = Counter(
        gold_table["classificacao_vigilancia_epidemiologica_municipal"].to_pylist()
    )
    assert tier_counts == {
        "POLO_SENTINELA_ALTA_COBERTURA_IMUNIZACAO": 3,
        "REDE_VIGILANCIA_EPIDEMIOLOGICA_CONSOLIDADA": 3,
        "TERRITORIO_PRESSAO_ENDEMICA_EM_MONITORAMENTO": 3,
        "ZONA_PRIORITARIA_CONTROLE_ARBOVIROSES_E_PNI": 3,
    }


def test_opendatasus_vigilancia_connector_methods_and_resolution() -> None:
    """Valida registro dinâmico, extract_table, normalize, resoluções e erros."""
    conn = get_connector("opendatasus_vigilancia")
    assert isinstance(conn, OpenDataSUSVigilanciaConnector)

    assert len(conn.extract_table("vigilancia_agravos_notificacao_sinan")) == 48
    assert (
        len(
            conn.extract_table(
                "features_opendatasus_vigilancia_epidemiologica_municipios_brasil"
            )
        )
        == 12
    )

    normalized = conn.normalize(
        [
            {
                "id_municipio": "4106900",
                "notificacoes_sindrome_gripal_esus_ano": 1500,
                "internacoes_srag_sivep_ano": 100,
                "taxa_incidencia_srag_100k_hab": 5.6,
                "internacoes_srag_uti_ano": 25,
                "obitos_srag_confirmados_ano": 9,
                "confirmacao_laboratorial_rt_pcr_srag_pct": 92.5,
            }
        ]
    )
    assert normalized.num_rows == 1
    assert normalized.to_pylist()[0]["id_municipio"] == "4106902"

    # IBGE-7, IBGE-6, alias, CEP-8, nome oficial e fallback determinístico
    assert conn.get_vigilancia_srag_respiratory_surveillance("3550308")["id_municipio"] == "3550308"
    assert conn.get_vigilancia_sipni_vaccination_coverage("410690")["id_municipio"] == "4106902"
    assert conn.get_vigilancia_cievs_response_capacity("4106900")["id_municipio"] == "4106902"
    assert conn.get_vigilancia_epidemiological_features("01001-000")["id_municipio"] == "3550308"
    assert conn.get_vigilancia_srag_respiratory_surveillance("São Paulo")["id_municipio"] == "3550308"

    fallback = conn.get_vigilancia_epidemiological_features("Campinas")
    assert fallback["id_municipio"] == "3509502"
    assert fallback["score_prontidao_vigilancia_epidemiologica"] > 0

    sinan_all = conn.list_vigilancia_sinan_notifiable_diseases("3550308")
    assert len(sinan_all) == 4
    sinan_arbo = conn.list_vigilancia_sinan_notifiable_diseases(
        "3550308", grupo_agravo="Arboviroses"
    )
    assert len(sinan_arbo) == 1
    assert "Arboviroses" in sinan_arbo[0]["grupo_agravo_sinan"]

    ranks = conn.list_top_vigilancia_surveillance_municipalities(limit=5, sigla_uf="SP")
    assert len(ranks) == 1
    assert ranks[0]["id_municipio"] == "3550308"

    with pytest.raises(ValueError):
        conn.get_vigilancia_srag_respiratory_surveillance("")
    with pytest.raises(ValueError):
        conn.get_vigilancia_epidemiological_features("99999999")
    with pytest.raises(ValueError):
        conn.list_top_vigilancia_surveillance_municipalities(limit=0)
    with pytest.raises(ValueError):
        conn.extract_table("tabela_inexistente")


def test_sdk_health_facade_and_brazildata_parity() -> None:
    """Valida paridade completa dos 6 métodos em BrazilData.health e BrazilData."""
    bdg = BrazilData()
    conn = OpenDataSUSVigilanciaConnector()

    assert (
        bdg.health.get_vigilancia_srag_respiratory_surveillance("3550308")
        == conn.get_vigilancia_srag_respiratory_surveillance("3550308")
    )
    assert (
        bdg.get_vigilancia_srag_respiratory_surveillance("3550308")
        == bdg.health.get_vigilancia_srag_respiratory_surveillance("3550308")
    )
    assert (
        bdg.list_vigilancia_sinan_notifiable_diseases("3550308", grupo_agravo="Arboviroses")
        == bdg.health.list_vigilancia_sinan_notifiable_diseases(
            "3550308", grupo_agravo="Arboviroses"
        )
    )
    assert (
        bdg.get_vigilancia_sipni_vaccination_coverage("4106900")
        == bdg.health.get_vigilancia_sipni_vaccination_coverage("4106900")
    )
    assert (
        bdg.get_vigilancia_cievs_response_capacity("3550308")
        == bdg.health.get_vigilancia_cievs_response_capacity("3550308")
    )
    assert (
        bdg.get_vigilancia_epidemiological_features("4106900")
        == bdg.health.get_vigilancia_epidemiological_features("4106900")
    )
    ranks = bdg.list_top_vigilancia_surveillance_municipalities(limit=4)
    assert len(ranks) == 4
    assert ranks == bdg.health.list_top_vigilancia_surveillance_municipalities(limit=4)


def test_cli_vigilancia_subcommands(capsys: pytest.CaptureFixture[str]) -> None:
    """Valida subcomandos CLI bdg vigilancia (JSON, texto e erro)."""
    assert main(["vigilancia", "srag", "3550308", "--json"]) == 0
    srag_json = json.loads(capsys.readouterr().out)
    assert srag_json["id_municipio"] == "3550308"

    assert main(["vigilancia", "sinan", "3550308", "--grupo", "Arboviroses", "--json"]) == 0
    sinan_json = json.loads(capsys.readouterr().out)
    assert len(sinan_json) == 1

    assert main(["vigilancia", "vacinacao", "4106900"]) == 0
    assert "OpenDataSUS SI-PNI Cobertura Vacinal" in capsys.readouterr().out

    assert main(["vigilancia", "cievs", "3550308"]) == 0
    assert "OpenDataSUS CIEVS/GAL-LACEN" in capsys.readouterr().out

    assert main(["vigilancia", "features", "01001000"]) == 0
    assert "Score Prontidão" in capsys.readouterr().out

    assert main(["vigilancia", "ranking", "--limit", "3"]) == 0
    assert "Prontidão Epidemiológica" in capsys.readouterr().out

    assert main(["vigilancia", "srag", "99999999"]) == 1
    _ = capsys.readouterr()


def test_fastapi_vigilancia_routes() -> None:
    """Valida endpoints FastAPI REST de opendatasus_vigilancia e tratamento de 400."""
    client = TestClient(create_app())

    r_top = client.get("/api/v1/health/vigilancia/top-municipalities?limit=3&uf=SP")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 1
    assert r_top.json()[0]["id_municipio"] == "3550308"

    r_feat = client.get("/api/v1/health/vigilancia/features/410690")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "4106902"

    r_sinan = client.get("/api/v1/health/vigilancia/sinan/3550308?grupo=Arboviroses")
    assert r_sinan.status_code == 200
    assert len(r_sinan.json()) == 1

    r_vac = client.get("/api/v1/health/vigilancia/vaccination/3550308")
    assert r_vac.status_code == 200
    assert r_vac.json()["id_municipio"] == "3550308"

    r_cievs = client.get("/api/v1/health/vigilancia/cievs/3550308")
    assert r_cievs.status_code == 200
    assert r_cievs.json()["id_municipio"] == "3550308"

    r_srag = client.get("/api/v1/health/vigilancia/srag/3550308")
    assert r_srag.status_code == 200
    assert r_srag.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/vigilancia/01001000")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"

    r_bad = client.get("/api/v1/health/vigilancia/features/99999999")
    assert r_bad.status_code == 400


def test_mcp_vigilancia_tools_and_error_handling() -> None:
    """Valida ferramentas MCP OpenDataSUS Vigilância, wrapper e erros."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_vigilancia_srag_respiratory_surveillance",
        "bdg_get_vigilancia_sipni_vaccination_coverage",
        "bdg_get_vigilancia_epidemiological_features",
        "bdg_list_top_vigilancia_surveillance_municipalities",
    ):
        assert expected in tool_names

    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_vigilancia_epidemiological_features",
            "arguments": {"id_municipio": "410690"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert res_call["result"]["isError"] is False
    text = res_call["result"]["content"][0]["text"]
    assert '<untrusted_gov_data source="opendatasus_vigilancia">' in text
    assert "4106902" in text

    req_srag = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_vigilancia_srag_respiratory_surveillance",
            "arguments": {"id_municipio": "01001000"},
        },
    }
    res_srag = dispatcher.handle_request(req_srag)
    assert res_srag["result"]["isError"] is False
    assert "3550308" in res_srag["result"]["content"][0]["text"]

    req_top = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "bdg_list_top_vigilancia_surveillance_municipalities",
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
            "name": "bdg_get_vigilancia_sipni_vaccination_coverage",
            "arguments": {"id_municipio": ""},
        },
    }
    res_err = dispatcher.handle_request(req_err)
    assert res_err["result"]["isError"] is True
