"""Tests for ANVISA CMED Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
    ANVISACmedConnector,
    _classify_cmed_tier,
    _compute_cmed_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_cmed_baseline() -> None:
    """Garante que os Parquets de baseline CMED existam em ambiente CI limpo."""
    conn = ANVISACmedConnector()
    if not (conn.silver_dir / "cmed_apresentacoes_registradas_municipio.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 CMED está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "saude"
        / "anvisa_cmed"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-ANVISA-CMED-001"
    assert data["source_id"] == "anvisa_cmed"
    tables = data["schema"]["tables"]
    assert "cmed_apresentacoes_registradas_municipio" in tables
    assert "cmed_precos_regulados_classe_terapeutica" in tables
    assert "cmed_compras_publicas_cap_pmvg" in tables
    assert "cmed_regime_tributario_icms_piscofins" in tables
    assert "features_cmed_precos_medicamentos_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida cálculo dos scores normalizados (0-100) e classificação dos polos CMED."""
    score_comp, score_ef = _compute_cmed_scores(
        participacao_genericos_similares_pct=74.5,
        participacao_lista_positiva_piscofins_pct=64.0,
        apresentacoes_comercializacao_2024_ativa_pct=82.5,
        conformidade_teto_pmvg_compras_sus_pct=96.4,
        economia_potencial_pmvg_sobre_pmc_pct=44.2,
        proporcao_apresentacoes_cap_pct=50.5,
    )
    assert 70.0 <= score_comp <= 100.0
    assert 68.0 <= score_ef <= 100.0
    assert (
        _classify_cmed_tier(score_eficiencia=score_ef, score_competitividade=score_comp)
        == "POLO_FARMACEUTICO_ALTA_CONFORMIDADE_CMED"
    )
    assert (
        _classify_cmed_tier(score_eficiencia=64.0, score_competitividade=65.0)
        == "REDE_REGULADA_CONSOLIDADA_CAP_PMVG"
    )
    assert (
        _classify_cmed_tier(score_eficiencia=58.0, score_competitividade=59.0)
        == "MERCADO_EM_QUALIFICACAO_COMPRAS_PUBLICAS"
    )
    assert (
        _classify_cmed_tier(score_eficiencia=52.0, score_competitividade=51.0)
        == "POLO_PRIORITARIO_AUDITORIA_PRECOS_CMED"
    )


def test_baseline_parquets_exist() -> None:
    """Valida materialização física e paridade de colunas dos Parquets CMED."""
    conn = ANVISACmedConnector()
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "saude"
        / "anvisa_cmed"
        / "contract.yaml"
    )
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))

    paths = {
        "cmed_apresentacoes_registradas_municipio": conn.silver_dir
        / "cmed_apresentacoes_registradas_municipio.parquet",
        "cmed_precos_regulados_classe_terapeutica": conn.silver_dir
        / "cmed_precos_regulados_classe_terapeutica.parquet",
        "cmed_compras_publicas_cap_pmvg": conn.silver_dir
        / "cmed_compras_publicas_cap_pmvg.parquet",
        "cmed_regime_tributario_icms_piscofins": conn.silver_dir
        / "cmed_regime_tributario_icms_piscofins.parquet",
        "features_cmed_precos_medicamentos_municipios_brasil": conn.gold_dir
        / "features_cmed_precos_medicamentos_municipios_brasil.parquet",
    }

    for tbl_name, path in paths.items():
        assert path.exists(), f"Parquet ausente: {path}"
        table = pq.read_table(path)
        expected_cols = [c["name"] for c in contract["schema"]["tables"][tbl_name]["columns"]]
        assert table.column_names == expected_cols
        min_rows = contract["schema"]["tables"][tbl_name]["min_expected_rows"]
        assert table.num_rows >= min_rows


def test_connector_methods_and_cnpj_resolution() -> None:
    """Testa registro dinâmico, métodos CMED e resolução por IBGE, CNPJ-14 e nome."""
    dynamic_conn = get_connector("anvisa_cmed")
    assert isinstance(dynamic_conn, ANVISACmedConnector)

    pres_ibge = dynamic_conn.get_presentations_summary("3550308")
    assert pres_ibge["id_municipio"] == "3550308"
    assert pres_ibge["cnpj_14"] == "61150819000120"
    assert pres_ibge["total_apresentacoes_ativas_cmed"] > 0

    pres_cnpj = dynamic_conn.get_presentations_summary("61.150.819/0001-20")
    assert pres_cnpj["id_municipio"] == "3550308"

    pres_name = dynamic_conn.get_presentations_summary("São Paulo")
    assert pres_name["id_municipio"] == "3550308"

    classes = dynamic_conn.list_therapeutic_class_prices(
        "3550308", classe_terapeutica_atc="Antineoplasicos"
    )
    assert len(classes) == 1
    assert "Antineoplasicos" in classes[0]["classe_terapeutica_atc"]

    pmvg = dynamic_conn.get_public_procurement_pmvg("3550308")
    assert pmvg["id_municipio"] == "3550308"
    assert pmvg["pmvg_medio_ponderado_brl"] < pmvg["pmc_medio_ponderado_aliquota_uf_brl"]

    tax = dynamic_conn.get_tax_regime_piscofins("3550308")
    assert tax["id_municipio"] == "3550308"
    assert tax["aliquota_icms_modal_uf_pct"] > 0.0

    features = dynamic_conn.get_cmed_drug_pricing_features("4106902")
    assert features["id_municipio"] == "4106902"
    assert 0.0 <= features["score_competitividade_genericos_cmed"] <= 100.0
    assert 0.0 <= features["score_eficiencia_regulatoria_pmvg_cap"] <= 100.0

    ranking = dynamic_conn.list_top_cmed_regulatory_efficiency_hubs(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_eficiencia_regulatoria_pmvg_cap"]
        >= ranking[-1]["score_eficiencia_regulatoria_pmvg_cap"]
    )


def test_sdk_parity() -> None:
    """Testa paridade das chamadas ANVISA CMED na fachada HealthFacade e BrazilData."""
    sdk = BrazilData()

    pres = sdk.get_cmed_presentations_summary("3550308")
    assert pres["id_municipio"] == "3550308"
    assert sdk.health.get_cmed_presentations_summary("3550308")["id_municipio"] == "3550308"

    classes = sdk.list_cmed_therapeutic_class_prices("3550308")
    assert len(classes) == 4

    pmvg = sdk.get_cmed_public_procurement_pmvg("3550308")
    assert pmvg["id_municipio"] == "3550308"

    taxes = sdk.get_cmed_tax_regime_piscofins("3550308")
    assert taxes["id_municipio"] == "3550308"

    feat = sdk.get_cmed_drug_pricing_features("4106902")
    assert feat["id_municipio"] == "4106902"

    ranks = sdk.list_top_cmed_regulatory_efficiency_hubs(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os 6 subcomandos CLI de cmed."""
    assert main(["cmed", "presentations", "3550308", "--json"]) == 0
    pres_data = json.loads(capsys.readouterr().out)
    assert pres_data["id_municipio"] == "3550308"

    assert main(["cmed", "classes", "3550308", "--classe", "Antineoplasicos"]) == 0
    assert "Antineoplasicos" in capsys.readouterr().out

    assert main(["cmed", "pmvg", "3550308"]) == 0
    assert "ANVISA CMED Compras Públicas CAP & PMVG" in capsys.readouterr().out

    assert main(["cmed", "taxes", "3550308"]) == 0
    assert "ANVISA CMED Regime Tributário ICMS & PIS/COFINS" in capsys.readouterr().out

    assert main(["cmed", "features", "4106902"]) == 0
    assert "Score Eficiência PMVG" in capsys.readouterr().out

    assert main(["cmed", "ranking", "--limit", "3"]) == 0
    assert "Polos Farmacêuticos CMED" in capsys.readouterr().out

    assert main(["cmed", "presentations", "99999999999999"]) == 1
    _ = capsys.readouterr()


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para ANVISA CMED."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/health-public/cmed/top-hubs?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/health-public/cmed/features/4106902")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "4106902"

    r_cls = client.get("/api/v1/health-public/cmed/classes/3550308?classe=Antineoplasicos")
    assert r_cls.status_code == 200
    assert len(r_cls.json()) == 1

    r_pmvg = client.get("/api/v1/health-public/cmed/pmvg/3550308")
    assert r_pmvg.status_code == 200
    assert r_pmvg.json()["id_municipio"] == "3550308"

    r_tax = client.get("/api/v1/health-public/cmed/taxes/3550308")
    assert r_tax.status_code == 200
    assert r_tax.json()["id_municipio"] == "3550308"

    r_pres = client.get("/api/v1/health-public/cmed/presentations/3550308")
    assert r_pres.status_code == 200
    assert r_pres.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/cmed/61150819000120")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"

    r_bad = client.get("/api/v1/health-public/cmed/features/99999999999999")
    assert r_bad.status_code == 400


def test_mcp_parity_and_error_handling() -> None:
    """Testa ferramentas MCP ANVISA CMED, wrap untrusted_gov_data e validação de erros."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_cmed_presentations_summary",
        "bdg_get_cmed_public_procurement_pmvg",
        "bdg_get_cmed_drug_pricing_features",
        "bdg_list_top_cmed_regulatory_efficiency_hubs",
    ):
        assert expected in tool_names

    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_cmed_drug_pricing_features",
            "arguments": {"id_municipio": "4106902"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert res_call["result"]["isError"] is False
    text = res_call["result"]["content"][0]["text"]
    assert '<untrusted_gov_data source="anvisa_cmed">' in text
    assert "4106902" in text

    req_pmvg = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_cmed_public_procurement_pmvg",
            "arguments": {"id_municipio": "61150819000120"},
        },
    }
    res_pmvg = dispatcher.handle_request(req_pmvg)
    assert res_pmvg["result"]["isError"] is False
    assert "3550308" in res_pmvg["result"]["content"][0]["text"]

    req_top = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "bdg_list_top_cmed_regulatory_efficiency_hubs",
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
            "name": "bdg_get_cmed_presentations_summary",
            "arguments": {"id_municipio": ""},
        },
    }
    res_err = dispatcher.handle_request(req_err)
    assert res_err["result"]["isError"] is True

    conn = ANVISACmedConnector()
    with pytest.raises(ValueError):
        conn.get_presentations_summary("")
    with pytest.raises(ValueError):
        conn.list_top_cmed_regulatory_efficiency_hubs(limit=0)
    with pytest.raises(ValueError):
        conn.get_cmed_drug_pricing_features("99999999999999")
