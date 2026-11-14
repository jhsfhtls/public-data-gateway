"""Tests for Capitais IPTU/ITBI Municipal Property Cadastre & Transactions Connector."""

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
from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
    CapitaisIptuItbiConnector,
    _classify_iptu_itbi_tier,
    _compute_iptu_itbi_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_iptu_itbi_baseline() -> None:
    """Garante que os 5 Parquets de baseline existam em ambiente CI limpo."""
    conn = CapitaisIptuItbiConnector()
    required = [
        conn.silver_dir / "iptu_itbi_cadastro_imobiliario_municipal.parquet",
        conn.silver_dir / "itbi_transacoes_imobiliarias_padrao.parquet",
        conn.silver_dir / "iptu_itbi_dinamica_precos_bairros.parquet",
        conn.silver_dir / "iptu_itbi_governanca_fiscal_geoespacial.parquet",
        conn.gold_dir / "features_iptu_itbi_mercado_imobiliario_capitais.parquet",
    ]
    if not all(path.exists() for path in required):
        conn.materialize_all()


def test_capitais_iptu_itbi_contract_validity() -> None:
    """Verifica a integridade do contrato ODCS v3.1 de capitais_iptu_itbi."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "imobiliario"
        / "capitais_iptu_itbi"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-CAPITAIS-IPTU-ITBI-001"
    assert data["source_id"] == "capitais_iptu_itbi"
    assert data["data_governance"]["lgpd_tier"] == "PUBLIC_NON_PERSONAL"
    assert data["data_governance"]["primary_keys"] == ["id_municipio", "sigla_uf"]

    tables = data["schema"]["tables"]
    expected_tables = (
        "iptu_itbi_cadastro_imobiliario_municipal",
        "itbi_transacoes_imobiliarias_padrao",
        "iptu_itbi_dinamica_precos_bairros",
        "iptu_itbi_governanca_fiscal_geoespacial",
        "features_iptu_itbi_mercado_imobiliario_capitais",
    )
    assert len(tables) == 5
    for table_name in expected_tables:
        assert table_name in tables
        col_names = {col["name"] for col in tables[table_name]["columns"]}
        assert {"id_municipio", "nome_municipio", "sigla_uf", "reference_date"}.issubset(
            col_names
        )


def test_capitais_iptu_itbi_score_and_tier_classification() -> None:
    """Valida cálculo determinístico de scores (0-100) e classificação nos 4 tiers."""
    score_din, score_gov = _compute_iptu_itbi_scores(
        preco_medio_m2_itbi_declarado_brl=11240.0,
        variacao_anual_preco_m2_itbi_pct=8.6,
        indice_liquidez_imobiliaria_itbi_pct=4.92,
        taxa_adimplencia_iptu_pct=91.4,
        cobertura_cadastral_georreferenciada_ctm_pct=96.4,
        integracao_iptu_sinter_cno_onr_pct=88.6,
        razao_venal_pgv_sobre_mercado_itbi=0.64,
    )
    assert score_din == 83.74
    assert score_gov == 89.78
    assert (
        _classify_iptu_itbi_tier(score_dinamismo=score_din, score_governanca=score_gov)
        == "METROPOLE_ALTA_LIQUIDEZ_E_GOVERNANCA_CTM"
    )
    assert (
        _classify_iptu_itbi_tier(score_dinamismo=67.5, score_governanca=82.5)
        == "MERCADO_IMOBILIARIO_CONSOLIDADO_PGV_ATUALIZADA"
    )
    assert (
        _classify_iptu_itbi_tier(score_dinamismo=58.0, score_governanca=74.5)
        == "MERCADO_URBANO_EM_EXPANSAO_CADASTRAL"
    )
    assert (
        _classify_iptu_itbi_tier(score_dinamismo=45.0, score_governanca=68.0)
        == "CAPITAL_PRIORITARIA_MODERNIZACAO_CTM_PGV"
    )


def test_capitais_iptu_itbi_baseline_parquets() -> None:
    """Valida os 5 Parquets Silver/Gold e a distribuição equilibrada nos 4 tiers."""
    conn = CapitaisIptuItbiConnector()
    specs = [
        (conn.silver_dir / "iptu_itbi_cadastro_imobiliario_municipal.parquet", 12, 16),
        (conn.silver_dir / "itbi_transacoes_imobiliarias_padrao.parquet", 48, 14),
        (conn.silver_dir / "iptu_itbi_dinamica_precos_bairros.parquet", 12, 15),
        (conn.silver_dir / "iptu_itbi_governanca_fiscal_geoespacial.parquet", 12, 15),
        (
            conn.gold_dir / "features_iptu_itbi_mercado_imobiliario_capitais.parquet",
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
    tier_counts = Counter(gold_table["classificacao_mercado_imobiliario_capital"].to_pylist())
    assert tier_counts == {
        "METROPOLE_ALTA_LIQUIDEZ_E_GOVERNANCA_CTM": 3,
        "MERCADO_IMOBILIARIO_CONSOLIDADO_PGV_ATUALIZADA": 3,
        "MERCADO_URBANO_EM_EXPANSAO_CADASTRAL": 3,
        "CAPITAL_PRIORITARIA_MODERNIZACAO_CTM_PGV": 3,
    }


def test_capitais_iptu_itbi_connector_methods_and_resolution() -> None:
    """Valida registro dinâmico, extract_table, normalize, resoluções e erros."""
    conn = get_connector("capitais_iptu_itbi")
    assert isinstance(conn, CapitaisIptuItbiConnector)

    assert len(conn.extract_table("itbi_transacoes_imobiliarias_padrao")) == 48
    assert (
        len(
            conn.extract_table(
                "features_iptu_itbi_mercado_imobiliario_capitais"
            )
        )
        == 12
    )

    normalized = conn.normalize(
        [
            {
                "id_municipio": "4106900",
                "inscricoes_imobiliarias_ativas_iptu": 865000,
                "area_construida_tributada_mil_m2": 134200.0,
                "valor_venal_total_bilhoes_brl": 542.0,
                "valor_venal_medio_m2_brl": 6698.0,
                "aliquota_media_efetiva_iptu_pct": 0.78,
                "arrecadacao_iptu_milhoes_brl": 1980.0,
                "taxa_adimplencia_iptu_pct": 89.2,
                "indice_atualizacao_pgv_pct": 92.5,
            }
        ]
    )
    assert normalized.num_rows == 1
    assert normalized.to_pylist()[0]["id_municipio"] == "4106902"

    # IBGE-7, IBGE-6, alias, CEP-8, nome oficial e fallback determinístico
    assert conn.get_iptu_itbi_property_cadastre("3550308")["id_municipio"] == "3550308"
    assert conn.get_iptu_itbi_price_dynamics("410690")["id_municipio"] == "4106902"
    assert conn.get_iptu_itbi_fiscal_governance("4106900")["id_municipio"] == "4106902"
    assert conn.get_iptu_itbi_real_estate_features("01001-000")["id_municipio"] == "3550308"
    assert conn.get_iptu_itbi_property_cadastre("São Paulo")["id_municipio"] == "3550308"

    fallback = conn.get_iptu_itbi_real_estate_features("Campinas")
    assert fallback["id_municipio"] == "3509502"
    assert fallback["score_dinamismo_mercado_imobiliario_itbi"] == 49.73
    assert fallback["score_governanca_cadastral_fiscal_iptu"] == 73.98

    tx_all = conn.list_itbi_transactions_by_segment("3550308")
    assert len(tx_all) == 4
    tx_com = conn.list_itbi_transactions_by_segment("3550308", segmento_imovel="Comercial")
    assert len(tx_com) == 1
    assert "Comercial" in tx_com[0]["segmento_imovel_itbi"]

    ranks = conn.list_top_iptu_itbi_capitals(limit=5, sigla_uf="SP")
    assert len(ranks) == 1
    assert ranks[0]["id_municipio"] == "3550308"

    with pytest.raises(ValueError):
        conn.get_iptu_itbi_property_cadastre("")
    with pytest.raises(ValueError):
        conn.get_iptu_itbi_real_estate_features("99999999")
    with pytest.raises(ValueError):
        conn.list_top_iptu_itbi_capitals(limit=0)
    with pytest.raises(ValueError):
        conn.extract_table("tabela_inexistente")


def test_sdk_real_estate_facade_iptu_itbi_parity() -> None:
    """Valida paridade completa dos 6 métodos em BrazilData.real_estate e BrazilData."""
    bdg = BrazilData()
    conn = CapitaisIptuItbiConnector()

    assert (
        bdg.real_estate.get_iptu_itbi_property_cadastre("3550308")
        == conn.get_iptu_itbi_property_cadastre("3550308")
    )
    assert (
        bdg.get_iptu_itbi_property_cadastre("3550308")
        == bdg.real_estate.get_iptu_itbi_property_cadastre("3550308")
    )
    assert (
        bdg.list_itbi_transactions_by_segment("3550308", segmento_imovel="Comercial")
        == bdg.real_estate.list_itbi_transactions_by_segment(
            "3550308", segmento_imovel="Comercial"
        )
    )
    assert (
        bdg.get_iptu_itbi_price_dynamics("4106900")
        == bdg.real_estate.get_iptu_itbi_price_dynamics("4106900")
    )
    assert (
        bdg.get_iptu_itbi_fiscal_governance("3550308")
        == bdg.real_estate.get_iptu_itbi_fiscal_governance("3550308")
    )
    assert (
        bdg.get_iptu_itbi_real_estate_features("4106900")
        == bdg.real_estate.get_iptu_itbi_real_estate_features("4106900")
    )
    ranks = bdg.list_top_iptu_itbi_capitals(limit=4)
    assert len(ranks) == 4
    assert ranks == bdg.real_estate.list_top_iptu_itbi_capitals(limit=4)


def test_cli_iptu_itbi_subcommands(capsys: pytest.CaptureFixture[str]) -> None:
    """Valida subcomandos CLI bdg iptu-itbi (JSON, texto e erro)."""
    assert main(["iptu-itbi", "cadastre", "3550308", "--json"]) == 0
    cad_json = json.loads(capsys.readouterr().out)
    assert cad_json["id_municipio"] == "3550308"

    assert main(["iptu-itbi", "transactions", "3550308", "--segmento", "Comercial", "--json"]) == 0
    tx_json = json.loads(capsys.readouterr().out)
    assert len(tx_json) == 1

    assert main(["iptu-itbi", "prices", "4106900"]) == 0
    assert "Capitais IPTU/ITBI Dinâmica de Preços" in capsys.readouterr().out

    assert main(["iptu-itbi", "governance", "3550308"]) == 0
    assert "Capitais IPTU/ITBI Governança CTM" in capsys.readouterr().out

    assert main(["iptu-itbi", "features", "01001000"]) == 0
    assert "Score Dinamismo ITBI" in capsys.readouterr().out

    assert main(["iptu-itbi", "ranking", "--limit", "3"]) == 0
    assert "Dinamismo Imobiliário ITBI & Governança CTM" in capsys.readouterr().out

    assert main(["iptu-itbi", "cadastre", "99999999"]) == 1
    _ = capsys.readouterr()


def test_fastapi_iptu_itbi_routes() -> None:
    """Valida endpoints FastAPI REST de capitais_iptu_itbi e tratamento de 400."""
    client = TestClient(create_app())

    r_top = client.get("/api/v1/real-estate/iptu-itbi/top-municipalities?limit=3&uf=SP")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 1
    assert r_top.json()[0]["id_municipio"] == "3550308"

    r_feat = client.get("/api/v1/real-estate/iptu-itbi/features/410690")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "4106902"

    r_tx = client.get("/api/v1/real-estate/iptu-itbi/transactions/3550308?segmento=Comercial")
    assert r_tx.status_code == 200
    assert len(r_tx.json()) == 1

    r_prices = client.get("/api/v1/real-estate/iptu-itbi/prices/3550308")
    assert r_prices.status_code == 200
    assert r_prices.json()["id_municipio"] == "3550308"

    r_gov = client.get("/api/v1/real-estate/iptu-itbi/governance/3550308")
    assert r_gov.status_code == 200
    assert r_gov.json()["id_municipio"] == "3550308"

    r_cad = client.get("/api/v1/real-estate/iptu-itbi/cadastre/3550308")
    assert r_cad.status_code == 200
    assert r_cad.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/iptu-itbi/01001000")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"

    r_bad = client.get("/api/v1/real-estate/iptu-itbi/features/99999999")
    assert r_bad.status_code == 400


def test_mcp_iptu_itbi_tools_and_error_handling() -> None:
    """Valida ferramentas MCP Capitais IPTU/ITBI, wrapper de segurança e erros."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_iptu_itbi_property_cadastre",
        "bdg_get_iptu_itbi_price_dynamics",
        "bdg_get_iptu_itbi_real_estate_features",
        "bdg_list_top_iptu_itbi_capitals",
    ):
        assert expected in tool_names

    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_iptu_itbi_real_estate_features",
            "arguments": {"id_municipio": "410690"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert res_call["result"]["isError"] is False
    text = res_call["result"]["content"][0]["text"]
    assert '<untrusted_gov_data source="capitais_iptu_itbi">' in text
    assert "4106902" in text

    req_cad = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_iptu_itbi_property_cadastre",
            "arguments": {"id_municipio": "01001000"},
        },
    }
    res_cad = dispatcher.handle_request(req_cad)
    assert res_cad["result"]["isError"] is False
    assert "3550308" in res_cad["result"]["content"][0]["text"]

    req_top = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "bdg_list_top_iptu_itbi_capitals",
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
            "name": "bdg_get_iptu_itbi_price_dynamics",
            "arguments": {"id_municipio": ""},
        },
    }
    res_err = dispatcher.handle_request(req_err)
    assert res_err["result"]["isError"] is True
