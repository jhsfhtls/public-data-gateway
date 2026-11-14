"""Offline unit and integration tests for Phase 09 Cross-Domain Gold marts & Feature Store."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.cross_domain import CrossDomainService
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _seed_cross_domain_duckdb(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE feature_store_cross_domain_municipio (
                id_municipio VARCHAR,
                nome_municipio VARCHAR,
                sigla_uf VARCHAR,
                regiao VARCHAR,
                feat_pib_mil_reais DOUBLE,
                feat_pib_per_capita_reais DOUBLE,
                feat_populacao_residente BIGINT,
                feat_score_propensao_b2b_composto DOUBLE,
                feat_score_risco_compliance_fiscal DOUBLE,
                feat_score_conectividade_5g DOUBLE,
                feat_score_esg_agro_resiliencia DOUBLE,
                feat_score_governanca_datalake DOUBLE,
                feat_score_saude_assistencial DOUBLE,
                feat_score_educacao_ideb DOUBLE,
                feat_selic_meta_asof DOUBLE,
                feat_ipca_12m_asof DOUBLE
            );
            INSERT INTO feature_store_cross_domain_municipio VALUES
                ('3550308', 'Sao Paulo', 'SP', 'Sudeste', 828980607.0, 67123.45, 11451999, 89.4, 18.2, 92.1, 64.5, 88.0, 81.3, 76.4, 14.25, 5.06),
                ('3304557', 'Rio de Janeiro', 'RJ', 'Sudeste', 359634752.0, 57890.12, 6211223, 81.7, 24.5, 87.4, 61.2, 82.5, 78.9, 71.0, 14.25, 5.06);

            CREATE TABLE mart_solution_packs_insights_brasil (
                pack_id VARCHAR,
                pack_name VARCHAR,
                sigla_uf VARCHAR,
                regiao VARCHAR,
                total_municipios_uf BIGINT,
                score_dinamismo_b2b_territorial DOUBLE,
                total_estabelecimentos_ativos BIGINT,
                valor_homologado_compras_uf DOUBLE,
                total_sancoes_ativas_uf BIGINT,
                score_oportunidade_pack_uf DOUBLE,
                recomendacao_operacional_pack VARCHAR,
                reference_period VARCHAR
            );
            INSERT INTO mart_solution_packs_insights_brasil VALUES
                ('P01', 'Modern Data Platform & Analytics (Lakehouse)', 'SP', 'Sudeste', 645, 86.2, 4200000, 950000000.0, 42, 87.1, 'ACELERACAO_COMERCIAL_PRIORITARIA', '2025-03'),
                ('P04', 'Automacao Inteligente de Onboarding & Compliance KYB', 'RJ', 'Sudeste', 92, 74.5, 1150000, 310000000.0, 65, 78.9, 'ALTA_DEMANDA_COMPLIANCE_TPRM', '2025-03');

            CREATE TABLE mart_cross_domain_territorio_b2b_macro_brasil (
                sigla_uf VARCHAR,
                regiao VARCHAR,
                qtd_municipios_uf BIGINT,
                score_dinamismo_b2b_territorial DOUBLE,
                total_estabelecimentos_ativos BIGINT,
                valor_homologado_compras_uf DOUBLE,
                total_sancoes_ativas_uf BIGINT,
                selic_meta_fim_mes_pct DOUBLE,
                ipca_acumulado_12m_pct DOUBLE,
                regime_politica_monetaria VARCHAR,
                macro_ano_mes_referencia VARCHAR
            );
            INSERT INTO mart_cross_domain_territorio_b2b_macro_brasil VALUES
                ('SP', 'Sudeste', 645, 86.2, 4200000, 950000000.0, 42, 14.25, 5.06, 'CONTRACIONISTA', '2025-03');

            CREATE TABLE mart_cross_domain_esg_agro_infra_brasil (
                id_municipio VARCHAR,
                nome_municipio VARCHAR,
                sigla_uf VARCHAR,
                score_esg_agro_resiliencia DOUBLE,
                score_infra_conectividade_5g DOUBLE,
                nivel_risco_hidrico VARCHAR
            );
            INSERT INTO mart_cross_domain_esg_agro_infra_brasil VALUES
                ('3550308', 'Sao Paulo', 'SP', 64.5, 92.1, 'MODERADO');
            """
        )
    return db_path


def test_cross_domain_service_and_validation(tmp_path: Path) -> None:
    db_path = _seed_cross_domain_duckdb(tmp_path / "data" / "brasil.duckdb")
    service = CrossDomainService(settings=GatewaySettings(data_dir=db_path.parent))

    feat = service.get_cross_domain_features("3550308")
    assert feat["nome_municipio"] == "Sao Paulo"
    assert feat["feat_score_propensao_b2b_composto"] == 89.4

    # 6-digit IBGE prefix lookup
    feat_6 = service.get_cross_domain_features("355030")
    assert feat_6["id_municipio"] == "3550308"

    top = service.list_top_cross_domain_municipalities(limit=5, sigla_uf="SP")
    assert len(top) == 1
    assert top[0]["sigla_uf"] == "SP"

    packs = service.get_solution_pack_insights("p01", sigla_uf="sp")
    assert len(packs) == 1
    assert packs[0]["pack_id"] == "P01"

    b2b = service.get_macro_territory_b2b("sp")
    assert b2b["regime_politica_monetaria"] == "CONTRACIONISTA"

    esg = service.get_esg_agro_infra("3550308")
    assert esg["score_infra_conectividade_5g"] == 92.1

    with pytest.raises(ValueError, match="Invalid IBGE"):
        service.get_cross_domain_features("123")
    with pytest.raises(ValueError, match="Invalid UF"):
        service.get_macro_territory_b2b("SAO")
    with pytest.raises(ValueError, match="Invalid Solution Pack"):
        service.get_solution_pack_insights("P99")


def test_cross_domain_sdk_facade(tmp_path: Path) -> None:
    db_path = _seed_cross_domain_duckdb(tmp_path / "data" / "brasil.duckdb")
    sdk = BrazilData(settings=GatewaySettings(data_dir=db_path.parent))

    assert sdk.get_cross_domain_features("3304557")["sigla_uf"] == "RJ"
    assert len(sdk.list_top_cross_domain_municipalities(limit=2)) == 2
    assert sdk.get_solution_pack_insights("P04")[0]["sigla_uf"] == "RJ"
    assert sdk.cross_domain.get_macro_territory_b2b("SP")["qtd_municipios_uf"] == 645


def test_cross_domain_fastapi_routes(tmp_path: Path) -> None:
    db_path = _seed_cross_domain_duckdb(tmp_path / "data" / "brasil.duckdb")
    client = TestClient(create_app(settings=GatewaySettings(data_dir=db_path.parent)))

    r1 = client.get("/api/v1/cross-domain/top-municipalities?limit=1&sigla_uf=SP")
    assert r1.status_code == 200
    assert r1.json()[0]["id_municipio"] == "3550308"

    r2 = client.get("/api/v1/cross-domain/features/3550308")
    assert r2.status_code == 200
    assert r2.json()["feat_selic_meta_asof"] == 14.25

    r3 = client.get("/api/v1/cross-domain/solution-packs/P01/insights?sigla_uf=SP")
    assert r3.status_code == 200
    assert r3.json()[0]["recomendacao_operacional_pack"] == "ACELERACAO_COMERCIAL_PRIORITARIA"

    r4 = client.get("/api/v1/cross-domain/territory-b2b/SP")
    assert r4.status_code == 200
    assert r4.json()["sigla_uf"] == "SP"

    r5 = client.get("/api/v1/cross-domain/esg-agro/3550308")
    assert r5.status_code == 200
    assert r5.json()["nivel_risco_hidrico"] == "MODERADO"

    r_bad = client.get("/api/v1/cross-domain/solution-packs/P99/insights")
    assert r_bad.status_code == 400


def test_cross_domain_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _seed_cross_domain_duckdb(tmp_path / "data" / "brasil.duckdb")
    monkeypatch.setenv("PDG_DUCKDB_PATH", str(db_path))

    for argv in (
        ["bdg", "cross-domain", "features", "3550308", "--json"],
        ["bdg", "cross-domain", "ranking", "--uf", "SP", "--json"],
        ["bdg", "cross-domain", "insights", "P01", "--uf", "SP", "--json"],
        ["bdg", "cross-domain", "territory-b2b", "SP", "--json"],
        ["bdg", "cross-domain", "esg-agro", "3550308", "--json"],
    ):
        monkeypatch.setattr(sys, "argv", argv)
        assert cli_main() == 0
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed


def test_cross_domain_mcp_tools(tmp_path: Path) -> None:
    db_path = _seed_cross_domain_duckdb(tmp_path / "data" / "brasil.duckdb")
    dispatcher = MCPDispatcher(settings=GatewaySettings(data_dir=db_path.parent))

    tools_resp = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_cross_domain_features",
        "bdg_list_top_cross_domain_municipalities",
        "bdg_get_solution_pack_insights",
    }.issubset(tool_names)

    for idx, (tool_name, arguments) in enumerate(
        (
            ("bdg_get_cross_domain_features", {"id_municipio": "3550308"}),
            ("bdg_list_top_cross_domain_municipalities", {"limit": 2, "sigla_uf": "SP"}),
            ("bdg_get_solution_pack_insights", {"pack_id": "P01", "sigla_uf": "SP"}),
        ),
        start=2,
    ):
        resp = dispatcher.handle_request(
            {
                "jsonrpc": "2.0",
                "id": idx,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": arguments},
            }
        )
        assert resp["result"]["isError"] is False
        text = resp["result"]["content"][0]["text"]
        assert "cross_domain_gold" in text
