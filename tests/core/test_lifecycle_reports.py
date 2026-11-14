"""Offline unit and integration tests for Phase 09.5 Lifecycle Sales & Delivery Reports (S01..S09)."""

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
from public_data_gateway.core.lifecycle_reports import STAGE_TABLES, LifecycleReportsService
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _seed_lifecycle_duckdb(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE mart_lifecycle_s01_gtm_icp_ranking (
                pack_id VARCHAR, pack_name VARCHAR, id_municipio VARCHAR, nome_municipio VARCHAR,
                sigla_uf VARCHAR, regiao VARCHAR, populacao_residente BIGINT, pib_per_capita_reais DOUBLE,
                score_icp_pack_municipio DOUBLE, tier_icp_abm VARCHAR, gancho_prospeccao_bant VARCHAR,
                reference_date VARCHAR
            );
            INSERT INTO mart_lifecycle_s01_gtm_icp_ranking VALUES
                ('P01', 'Modern Data Platform', '3550308', 'Sao Paulo', 'SP', 'Sudeste', 11451999, 67123.45, 88.7, 'TIER_1_ABM_PRIORITARIO', 'Alta maturidade de dados', '2025-03');

            CREATE TABLE mart_lifecycle_s02_discovery_dores_roi (
                pack_id VARCHAR, pack_name VARCHAR, sigla_uf VARCHAR, regiao VARCHAR,
                total_municipios_uf BIGINT, score_oportunidade_pack_uf DOUBLE, dor_latente_principal VARCHAR,
                metrica_dor_quantificada VARCHAR, impacto_financeiro_estimado_brl DOUBLE,
                escopo_poc_14d_sugerido VARCHAR, reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s02_discovery_dores_roi VALUES
                ('P01', 'Modern Data Platform', 'SP', 'Sudeste', 645, 87.1, 'Dados fragmentados', '4200000 estabelecimentos', 23750000.0, 'PoC 14d Medallion + MCP', '2025-03');

            CREATE TABLE mart_lifecycle_s03_pricing_benchmarks_tam (
                pack_id VARCHAR, pack_name VARCHAR, sigla_uf VARCHAR, regiao VARCHAR,
                tam_empresas_ativas_uf BIGINT, sam_compras_publicas_uf_brl DOUBLE,
                economia_media_licitatoria_pct DOUBLE, ancora_roi_estimado_pct DOUBLE,
                faixa_payback_meses VARCHAR, reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s03_pricing_benchmarks_tam VALUES
                ('P01', 'Modern Data Platform', 'SP', 'Sudeste', 4200000, 950000000.0, 11.4, 35.06, '2 a 4 meses', '2025-03');

            CREATE TABLE mart_lifecycle_s04_legal_compliance_dd (
                pack_id VARCHAR, pack_name VARCHAR, sigla_uf VARCHAR, regiao VARCHAR,
                total_sancoes_ativas_uf BIGINT, valor_multas_ativas_uf DOUBLE,
                indice_risco_compliance_uf DOUBLE, indice_governanca_dados_uf DOUBLE,
                nivel_rigor_due_diligence VARCHAR, clausula_dpa_lgpd_recomendada VARCHAR,
                reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s04_legal_compliance_dd VALUES
                ('P01', 'Modern Data Platform', 'SP', 'Sudeste', 42, 1200000.0, 18.2, 88.0, 'RIGOR_MODERADO_PADRAO_KYB', 'Kit padrao NDA + MSA + SOW + DPA', '2025-03');

            CREATE TABLE mart_lifecycle_s05_onboarding_quickwins (
                pack_id VARCHAR, pack_name VARCHAR, datasets_prontos_silver_gold VARCHAR,
                endpoints_prioritarios VARCHAR, quick_win_14_dias VARCHAR,
                tempo_estimado_ativacao_horas INTEGER, reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s05_onboarding_quickwins VALUES
                ('P01', 'Modern Data Platform', 'feature_store_cross_domain_municipio', '/api/v1/cross-domain/features/{id_municipio}', 'Lakehouse operacional em < 14 dias', 16, '2025-03');

            CREATE TABLE mart_lifecycle_s06_delivery_feature_blueprint (
                pack_id VARCHAR, pack_name VARCHAR, features_chave_feature_store VARCHAR,
                blueprint_arquitetura_entrega VARCHAR, sprints_estimadas_delivery INTEGER,
                reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s06_delivery_feature_blueprint VALUES
                ('P01', 'Modern Data Platform', 'feat_score_propensao_b2b_composto', 'dbt + Parquet ZSTD + DuckDB', 4, '2025-03');

            CREATE TABLE mart_lifecycle_s07_uat_quality_scorecard (
                pack_id VARCHAR, pack_name VARCHAR, total_municipios_cobertos BIGINT,
                total_ufs_cobertas BIGINT, avg_maturidade_datalake DOUBLE,
                avg_propensao_b2b DOUBLE, cobertura_documentacao_odcs_pct DOUBLE,
                tolerancia_erro_paridade_sql_python DOUBLE, criterio_aceite_uat_dod VARCHAR,
                reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s07_uat_quality_scorecard VALUES
                ('P01', 'Modern Data Platform', 5570, 27, 74.2, 68.5, 100.0, 0.0, '100% testes dbt e pytest aprovados', '2025-03');

            CREATE TABLE mart_lifecycle_s08_sre_freshness_slos (
                pack_id VARCHAR, pack_name VARCHAR, cadencia_ingestao_recomendada VARCHAR,
                freshness_sla_horas INTEGER, slo_disponibilidade_pct DOUBLE,
                politica_resiliencia_sre VARCHAR, reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s08_sre_freshness_slos VALUES
                ('P01', 'Modern Data Platform', 'SEMANAL_CONSOLIDADA', 168, 99.5, 'Regressao live com rotacao de IDs', '2025-03');

            CREATE TABLE mart_lifecycle_s09_cs_upsell_matrix (
                pack_id VARCHAR, pack_name VARCHAR, sigla_uf VARCHAR, regiao VARCHAR,
                score_oportunidade_pack_uf DOUBLE, next_recommended_pack_id VARCHAR,
                tese_expansao_arr VARCHAR, score_propensao_upsell DOUBLE,
                recomendacao_operacional_pack VARCHAR, reference_period VARCHAR
            );
            INSERT INTO mart_lifecycle_s09_cs_upsell_matrix VALUES
                ('P01', 'Modern Data Platform', 'SP', 'Sudeste', 87.1, 'P02', 'Evoluir para Agentes IA com MCP Server', 74.04, 'ACELERACAO_COMERCIAL_PRIORITARIA', '2025-03');
            """
        )
    return db_path


def test_lifecycle_service_all_stages_and_dossier(tmp_path: Path) -> None:
    db_path = _seed_lifecycle_duckdb(tmp_path / "data" / "brasil.duckdb")
    svc = LifecycleReportsService(settings=GatewaySettings(data_dir=db_path.parent))

    for stage_id in STAGE_TABLES:
        rows = svc.get_stage_report(stage_id=stage_id, pack_id="p01", sigla_uf="sp")
        assert len(rows) == 1
        assert rows[0]["pack_id"] == "P01"

    dossier = svc.build_sales_dossier(pack_id="P01", sigla_uf="SP")
    assert set(dossier["stages"]) == set(STAGE_TABLES)
    md = svc.format_dossier_markdown(dossier)
    assert "Dossie Comercial & Entrega — P01 (SP)" in md
    assert "**S09**" in md

    with pytest.raises(ValueError, match="Invalid lifecycle stage"):
        svc.get_stage_report("S99", "P01")


def test_lifecycle_sdk_fastapi_cli_mcp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _seed_lifecycle_duckdb(tmp_path / "data" / "brasil.duckdb")
    settings = GatewaySettings(data_dir=db_path.parent)

    sdk = BrazilData(settings=settings)
    assert sdk.lifecycle.get_stage_report("S01", "P01", sigla_uf="SP")[0]["id_municipio"] == "3550308"
    assert sdk.lifecycle.build_sales_dossier("P01", "SP")["stages"]["S09"][0]["next_recommended_pack_id"] == "P02"

    client = TestClient(create_app(settings=settings))
    r1 = client.get("/api/v1/lifecycle/stages/S03?pack_id=P01&uf=SP")
    assert r1.status_code == 200
    assert r1.json()[0]["faixa_payback_meses"] == "2 a 4 meses"

    r2 = client.get("/api/v1/lifecycle/dossier/P01?uf=SP")
    assert r2.status_code == 200
    assert set(r2.json()["stages"]) == set(STAGE_TABLES)

    monkeypatch.setenv("PDG_DUCKDB_PATH", str(db_path))
    monkeypatch.setattr(sys, "argv", ["bdg", "lifecycle", "stage", "S02", "--pack", "P01", "--uf", "SP", "--json"])
    assert cli_main() == 0
    assert json.loads(capsys.readouterr().out)[0]["pack_id"] == "P01"

    monkeypatch.setattr(sys, "argv", ["bdg", "lifecycle", "dossier", "P01", "--uf", "SP", "--format", "markdown"])
    assert cli_main() == 0
    assert "Dossie Comercial & Entrega" in capsys.readouterr().out

    dispatcher = MCPDispatcher(settings=settings)
    tools = {t["name"] for t in dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})["result"]["tools"]}
    assert {"bdg_get_lifecycle_stage_report", "bdg_generate_solution_dossier"}.issubset(tools)

    resp = dispatcher.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "bdg_generate_solution_dossier", "arguments": {"pack_id": "P01", "sigla_uf": "SP"}},
        }
    )
    assert resp["result"]["isError"] is False
    assert "gtm_lifecycle_gold" in resp["result"]["content"][0]["text"]
