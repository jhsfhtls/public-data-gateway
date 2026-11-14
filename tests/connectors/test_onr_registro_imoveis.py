"""Tests for ONR Registro de Imóveis SREI Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
    ONRRegistroImoveisConnector,
    _classify_onr_tier,
    _compute_onr_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_onr_baseline() -> None:
    """Garante que os Parquets de baseline ONR/SREI existam em ambiente CI limpo."""
    conn = ONRRegistroImoveisConnector()
    if not (conn.silver_dir / "onr_matriculas_acervo_municipio.parquet").exists():
        conn.materialize_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 ONR Registro de Imóveis está conforme."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "imobiliario"
        / "onr_registro_imoveis"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-ONR-REGISTRO-IMOVEIS-001"
    assert data["source_id"] == "onr_registro_imoveis"
    tables = data["schema"]["tables"]
    assert "onr_matriculas_acervo_municipio" in tables
    assert "onr_transacoes_atos_natureza" in tables
    assert "onr_garantias_credito_imobiliario" in tables
    assert "onr_interoperabilidade_serp_eficiencia" in tables
    assert "features_onr_registro_imoveis_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida cálculo dos scores normalizados (0-100) e classificação dos 4 tiers ONR."""
    score_mat, score_liq = _compute_onr_scores(
        taxa_conversao_cnm_digital_pct=94.5,
        indice_integracao_serp_saec_pct=96.2,
        proporcao_titulos_nato_digitais_extrato_pct=86.0,
        prazo_medio_qualificacao_registral_dias=2.1,
        novas_matriculas_abertas_ano=168500,
        total_matriculas_ativas_cnm=4680000,
        participacao_sfi_sbpe_pmcmv_pct=74.5,
        taxa_execucao_garantia_fiduciaria_pct=1.35,
    )
    assert 86.0 <= score_mat <= 100.0
    assert 70.0 <= score_liq <= 100.0
    assert (
        _classify_onr_tier(score_maturidade_digital=score_mat)
        == "POLO_REGISTRAL_DIGITAL_ALTA_LIQUIDEZ_SERP"
    )
    assert (
        _classify_onr_tier(score_maturidade_digital=81.0)
        == "MERCADO_IMOBILIARIO_CONSOLIDADO_INTEGRADO_SREI"
    )
    assert (
        _classify_onr_tier(score_maturidade_digital=72.0)
        == "REDE_REGISTRAL_EM_TRANSICAO_ELETRONICA"
    )
    assert (
        _classify_onr_tier(score_maturidade_digital=60.0)
        == "TERRITORIO_PRIORITARIO_REGULARIZACAO_FUNDIARIA"
    )


def test_baseline_parquets_exist() -> None:
    """Valida materialização física e paridade de colunas dos Parquets ONR/SREI."""
    conn = ONRRegistroImoveisConnector()
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "imobiliario"
        / "onr_registro_imoveis"
        / "contract.yaml"
    )
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))

    paths = {
        "onr_matriculas_acervo_municipio": conn.silver_dir
        / "onr_matriculas_acervo_municipio.parquet",
        "onr_transacoes_atos_natureza": conn.silver_dir
        / "onr_transacoes_atos_natureza.parquet",
        "onr_garantias_credito_imobiliario": conn.silver_dir
        / "onr_garantias_credito_imobiliario.parquet",
        "onr_interoperabilidade_serp_eficiencia": conn.silver_dir
        / "onr_interoperabilidade_serp_eficiencia.parquet",
        "features_onr_registro_imoveis_municipios_brasil": conn.gold_dir
        / "features_onr_registro_imoveis_municipios_brasil.parquet",
    }

    for tbl_name, path in paths.items():
        assert path.exists(), f"Parquet ausente: {path}"
        table = pq.read_table(path)
        expected_cols = [c["name"] for c in contract["schema"]["tables"][tbl_name]["columns"]]
        assert table.column_names == expected_cols
        min_rows = contract["schema"]["tables"][tbl_name]["min_expected_rows"]
        assert table.num_rows >= min_rows


def test_connector_methods_normalize_and_resolution() -> None:
    """Testa registro dinâmico, extract_table, normalize e resolução IBGE/alias/CEP-8/nome."""
    dynamic_conn = get_connector("onr_registro_imoveis")
    assert isinstance(dynamic_conn, ONRRegistroImoveisConnector)

    acervo_rows = dynamic_conn.extract_table("onr_matriculas_acervo_municipio")
    assert len(acervo_rows) == 12

    norm_tbl = dynamic_conn.normalize(acervo_rows[:2])
    assert norm_tbl.num_rows == 2
    assert norm_tbl["id_municipio"].to_pylist()[0] == "3550308"

    reg_ibge = dynamic_conn.get_onr_property_registry("3550308")
    assert reg_ibge["id_municipio"] == "3550308"
    assert reg_ibge["cep_8"] == "01001000"
    assert reg_ibge["total_matriculas_ativas_cnm"] > 4_000_000

    reg_cep = dynamic_conn.get_onr_property_registry("01001-000")
    assert reg_cep["id_municipio"] == "3550308"

    reg_alias = dynamic_conn.get_onr_property_registry("4106900")
    assert reg_alias["id_municipio"] == "4106902"

    reg_name = dynamic_conn.get_onr_property_registry("São Paulo")
    assert reg_name["id_municipio"] == "3550308"

    tx_filtered = dynamic_conn.list_onr_transactions_by_nature(
        "4106902", natureza_ato="Compra_Venda"
    )
    assert len(tx_filtered) == 1
    assert tx_filtered[0]["natureza_ato_registral"] == "Compra_Venda_Transmissao"

    credit = dynamic_conn.get_onr_real_estate_credit_guarantees("4106902")
    assert credit["id_municipio"] == "4106902"
    assert credit["contratos_alienacao_fiduciaria_ano"] > 20_000

    serp = dynamic_conn.get_onr_serp_interoperability("4106902")
    assert serp["id_municipio"] == "4106902"
    assert serp["indice_integracao_serp_saec_pct"] >= 90.0

    features = dynamic_conn.get_onr_real_estate_features("3550308")
    assert features["id_municipio"] == "3550308"
    assert (
        features["classificacao_mercado_registral_imobiliario"]
        == "POLO_REGISTRAL_DIGITAL_ALTA_LIQUIDEZ_SERP"
    )
    assert 0.0 <= features["score_maturidade_digital_srei_serp"] <= 100.0
    assert 0.0 <= features["score_liquidez_seguranca_imobiliaria"] <= 100.0

    ranking = dynamic_conn.list_top_onr_real_estate_municipalities(limit=5)
    assert len(ranking) == 5
    assert (
        ranking[0]["score_maturidade_digital_srei_serp"]
        >= ranking[-1]["score_maturidade_digital_srei_serp"]
    )


def test_sdk_parity() -> None:
    """Testa paridade ONR na fachada RealEstateFacade, JudicialFacade e BrazilData."""
    sdk = BrazilData()
    assert isinstance(sdk.judicial.onr_connector, ONRRegistroImoveisConnector)

    reg = sdk.get_onr_property_registry("3550308")
    assert reg["id_municipio"] == "3550308"
    assert sdk.real_estate.get_onr_property_registry("3550308")["id_municipio"] == "3550308"

    txs = sdk.list_onr_transactions_by_nature("3550308")
    assert len(txs) == 4

    credit = sdk.get_onr_real_estate_credit_guarantees("3550308")
    assert credit["id_municipio"] == "3550308"

    serp = sdk.get_onr_serp_interoperability("3550308")
    assert serp["id_municipio"] == "3550308"

    feat = sdk.get_onr_real_estate_features("4106902")
    assert feat["id_municipio"] == "4106902"

    ranks = sdk.list_top_onr_real_estate_municipalities(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os 6 subcomandos CLI de onr."""
    assert main(["onr", "registry", "3550308", "--json"]) == 0
    reg_data = json.loads(capsys.readouterr().out)
    assert reg_data["id_municipio"] == "3550308"

    assert main(["onr", "transactions", "4106902", "--natureza", "Compra_Venda"]) == 0
    assert "Compra_Venda_Transmissao" in capsys.readouterr().out

    assert main(["onr", "credit", "3550308"]) == 0
    assert "ONR Garantias e Crédito Imobiliário" in capsys.readouterr().out

    assert main(["onr", "serp", "3550308"]) == 0
    assert "ONR Interoperabilidade SERP/SAEC" in capsys.readouterr().out

    assert main(["onr", "features", "4106902"]) == 0
    assert "Score Maturidade SERP" in capsys.readouterr().out

    assert main(["onr", "ranking", "--limit", "3"]) == 0
    assert "Maturidade Digital SERP" in capsys.readouterr().out

    assert main(["onr", "registry", "99999999"]) == 1
    _ = capsys.readouterr()


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para ONR Registro de Imóveis."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/real-estate/onr/top-municipalities?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/real-estate/onr/features/4106902")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "4106902"

    r_tx = client.get("/api/v1/real-estate/onr/transactions/4106902?natureza=Compra_Venda")
    assert r_tx.status_code == 200
    assert len(r_tx.json()) == 1

    r_cr = client.get("/api/v1/real-estate/onr/credit/3550308")
    assert r_cr.status_code == 200
    assert r_cr.json()["id_municipio"] == "3550308"

    r_srp = client.get("/api/v1/real-estate/onr/serp/3550308")
    assert r_srp.status_code == 200
    assert r_srp.json()["id_municipio"] == "3550308"

    r_reg = client.get("/api/v1/real-estate/onr/registry/3550308")
    assert r_reg.status_code == 200
    assert r_reg.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/onr/01001000")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"

    r_bad = client.get("/api/v1/real-estate/onr/features/99999999")
    assert r_bad.status_code == 400


def test_mcp_parity_and_error_handling() -> None:
    """Testa ferramentas MCP ONR, wrap untrusted_gov_data e validação de erros."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_onr_property_registry",
        "bdg_get_onr_real_estate_credit_guarantees",
        "bdg_get_onr_real_estate_features",
        "bdg_list_top_onr_real_estate_municipalities",
    ):
        assert expected in tool_names

    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_onr_real_estate_features",
            "arguments": {"id_municipio": "3550308"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert res_call["result"]["isError"] is False
    text = res_call["result"]["content"][0]["text"]
    assert '<untrusted_gov_data source="onr_registro_imoveis">' in text
    assert "3550308" in text

    req_cr = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_onr_real_estate_credit_guarantees",
            "arguments": {"id_municipio": "01001000"},
        },
    }
    res_cr = dispatcher.handle_request(req_cr)
    assert res_cr["result"]["isError"] is False
    assert "3550308" in res_cr["result"]["content"][0]["text"]

    req_top = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "bdg_list_top_onr_real_estate_municipalities",
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
            "name": "bdg_get_onr_property_registry",
            "arguments": {"id_municipio": ""},
        },
    }
    res_err = dispatcher.handle_request(req_err)
    assert res_err["result"]["isError"] is True

    conn = ONRRegistroImoveisConnector()
    with pytest.raises(ValueError):
        conn.get_onr_property_registry("")
    with pytest.raises(ValueError):
        conn.list_top_onr_real_estate_municipalities(limit=0)
    with pytest.raises(ValueError):
        conn.extract_table("tabela_inexistente")
    with pytest.raises(ValueError):
        conn.get_onr_real_estate_features("99999999")
