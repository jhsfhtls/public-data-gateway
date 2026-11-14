{{ config(materialized='table') }}

WITH packs AS (
    SELECT DISTINCT pack_id, pack_name
    FROM {{ ref('mart_solution_packs_insights_brasil') }}
)

SELECT
    pack_id,
    pack_name,
    CASE pack_id
        WHEN 'P01' THEN 'feat_score_propensao_b2b_composto, feat_score_governanca_datalake, pib_per_capita_reais'
        WHEN 'P02' THEN 'feat_score_governanca_datalake, feat_score_conectividade_5g, feat_score_propensao_b2b_composto'
        WHEN 'P03' THEN 'feat_score_conectividade_5g, is_capital, populacao_residente, feat_score_socioeconomico'
        WHEN 'P04' THEN 'feat_score_risco_compliance_fiscal, feat_score_dinamismo_b2b_uf, feat_score_propensao_b2b_composto'
        WHEN 'P05' THEN 'macro_selic_asof_pct, macro_ipca_12m_asof_pct, macro_juros_real_asof_pct, nota_capag_final'
        WHEN 'P06' THEN 'feat_score_socioeconomico, feat_score_dinamica_emprego, feat_score_saude_assistencial'
        WHEN 'P07' THEN 'feat_score_risco_compliance_fiscal, razao_dcl_rcl_pct, feat_score_dinamismo_b2b_uf'
        WHEN 'P08' THEN 'feat_score_governanca_datalake, feat_score_educacao_ideb, tier_maturidade_cross_domain'
        WHEN 'P09' THEN 'feat_score_esg_agro_resiliencia, is_amazonia_legal, feat_score_conectividade_5g'
        ELSE 'feat_score_governanca_datalake, feat_score_saude_assistencial, tier_maturidade_cross_domain'
    END AS features_chave_feature_store,
    CASE pack_id
        WHEN 'P01' THEN 'dbt/models/marts/cross_domain/ + Parquet ZSTD + DuckDB read-only'
        WHEN 'P02' THEN 'MCPDispatcher + FastAPI + wrap_untrusted_gov_data guardrails'
        WHEN 'P03' THEN 'GeoParquet + IBGE Malhas/CNEFE + ANATEL 5G'
        WHEN 'P04' THEN 'BrasilAPI + RFB CNPJ + CGU CEIS/CNEP + pipeline assincrono'
        WHEN 'P05' THEN 'BCB SGS/PTAX + ASOF JOIN temporal + series deflacionadas'
        WHEN 'P06' THEN 'Agregacoes LGPD-safe por UF/CNAE/municipio sem PII'
        WHEN 'P07' THEN 'CEIS/CNEP/PEP + CNJ DataJud + TST CNDT + PNCP'
        WHEN 'P08' THEN 'Contratos ODCS v3.1 + testes dbt + lint_dbt_docs.py'
        WHEN 'P09' THEN 'CAR/SICAR + ANA HidroWeb + TerraBrasilis + MapBiomas + ZARC'
        ELSE 'Catalogo DuckDB + Healthchecks + Regressao Live com rotacao de IDs'
    END AS blueprint_arquitetura_entrega,
    CASE
        WHEN pack_id IN ('P01', 'P02', 'P09') THEN 4
        WHEN pack_id IN ('P04', 'P05', 'P07') THEN 3
        ELSE 2
    END AS sprints_estimadas_delivery,
    '2025-03' AS reference_period
FROM packs
ORDER BY pack_id ASC
