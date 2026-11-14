{{ config(materialized='table') }}

WITH packs AS (
    SELECT DISTINCT pack_id, pack_name
    FROM {{ ref('mart_solution_packs_insights_brasil') }}
)

SELECT
    pack_id,
    pack_name,
    CASE pack_id
        WHEN 'P01' THEN 'feature_store_cross_domain_municipio, mart_cross_domain_territorio_b2b_macro_brasil, mart_pib_municipal_observacoes'
        WHEN 'P02' THEN 'MCP Server (35+ tools), wrap_untrusted_gov_data, mart_solution_packs_insights_brasil'
        WHEN 'P03' THEN 'ibge_localidades, ibge_cnefe, ibge_malhas, brasilapi_core, anatel_mosaico'
        WHEN 'P04' THEN 'brasilapi_core, rfb_cnpj_lakehouse, rfb_simples_mei, cgu_ceis_cnep'
        WHEN 'P05' THEN 'bcb_sgs, bcb_olinda_ptax, mart_cenario_macroeconomico_brasil, stn_tesouro_transparente'
        WHEN 'P06' THEN 'mart_cross_domain_capital_humano_saude_brasil, mart_densidade_empresarial_brasil'
        WHEN 'P07' THEN 'cgu_ceis_cnep, cgu_pep, cnj_datajud, tst_cndt, pncp_contratacoes'
        WHEN 'P08' THEN 'lint_dbt_docs.py, mart_transparencia_dados_abertos_ckan_brasil, mart_maturidade_datalake_federado_brasil'
        WHEN 'P09' THEN 'mart_cross_domain_esg_agro_infra_brasil, sfb_sicar, ana_hidroweb_telemetria, mapbiomas_cobertura, mapa_zarc'
        ELSE 'state/project_progress.json, catalog/sources_registry.yaml, mart_maturidade_datalake_federado_brasil'
    END AS datasets_prontos_silver_gold,
    CASE pack_id
        WHEN 'P01' THEN '/api/v1/cross-domain/features/{id_municipio}, /api/v1/cross-domain/top-municipalities'
        WHEN 'P02' THEN 'MCP tools/list, bdg_get_cross_domain_features, bdg_get_solution_pack_insights'
        WHEN 'P03' THEN '/api/v1/geo/malhas/*, /api/v1/geo/anatel/*, /api/v1/cross-domain/esg-agro/{id_municipio}'
        WHEN 'P04' THEN '/api/v1/companies/{cnpj}, /api/v1/compliance/*, /api/v1/cross-domain/territory-b2b/{uf}'
        WHEN 'P05' THEN '/api/v1/macro/*, /api/v1/cross-domain/features/{id_municipio}'
        WHEN 'P06' THEN '/api/v1/cross-domain/solution-packs/{pack_id}/insights'
        WHEN 'P07' THEN '/api/v1/compliance/*, /api/v1/procurement/*'
        WHEN 'P08' THEN '/api/v1/metaportals/dadosgov/*, /api/v1/lifecycle/stages/S07'
        WHEN 'P09' THEN '/api/v1/cross-domain/esg-agro/{id_municipio}, /api/v1/environmental/*'
        ELSE '/api/v1/health, /api/v1/lifecycle/stages/S08'
    END AS endpoints_prioritarios,
    CASE pack_id
        WHEN 'P01' THEN 'Lakehouse DuckDB/Parquet operacional com painel executivo municipal em < 14 dias'
        WHEN 'P02' THEN 'Agente IA conectado ao MCP Server consultando dados publicos com protecao contra prompt injection'
        WHEN 'P03' THEN 'Enriquecimento de base de clientes por CEP/IBGE e cobertura 5G em lote'
        WHEN 'P04' THEN 'Pipeline automatizado de onboarding KYB com consulta CNPJ + sancoes CGU'
        WHEN 'P05' THEN 'Serie historica Selic/IPCA/PTAX acoplada a contratos com ASOF join'
        WHEN 'P06' THEN 'Benchmark competitivo anonimizado por UF e CNAE em conformidade LGPD'
        WHEN 'P07' THEN 'Matriz de risco de fornecedores (CEIS/CNEP/CNDT/PNCP) automatizada'
        WHEN 'P08' THEN 'Auditoria automatizada de contratos ODCS v3.1 e 100% de cobertura documental'
        WHEN 'P09' THEN 'Mapa de risco hidrico, fundiario (CAR) e conectividade rural por municipio'
        ELSE 'Monitoramento continuo de frescor de fontes e integridade de schemas'
    END AS quick_win_14_dias,
    CASE
        WHEN pack_id IN ('P01', 'P04', 'P05') THEN 16
        WHEN pack_id IN ('P02', 'P07', 'P09') THEN 24
        ELSE 20
    END AS tempo_estimado_ativacao_horas,
    '2025-03' AS reference_period
FROM packs
ORDER BY pack_id ASC
