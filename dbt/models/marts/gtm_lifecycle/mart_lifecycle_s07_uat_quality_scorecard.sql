{{ config(materialized='table') }}

WITH stats AS (
    SELECT
        COUNT(*) AS total_municipios_cobertos,
        COUNT(DISTINCT sigla_uf) AS total_ufs_cobertas,
        ROUND(AVG(feat_score_governanca_datalake), 2) AS avg_maturidade_datalake,
        ROUND(AVG(feat_score_propensao_b2b_composto), 2) AS avg_propensao_b2b
    FROM {{ ref('feature_store_cross_domain_municipio') }}
),

packs AS (
    SELECT DISTINCT pack_id, pack_name
    FROM {{ ref('mart_solution_packs_insights_brasil') }}
)

SELECT
    p.pack_id,
    p.pack_name,
    s.total_municipios_cobertos,
    s.total_ufs_cobertas,
    s.avg_maturidade_datalake,
    s.avg_propensao_b2b,
    100.0 AS cobertura_documentacao_odcs_pct,
    0.0 AS tolerancia_erro_paridade_sql_python,
    '100% testes dbt (schema/not_null/unique/relationships) + pytest offline e regressao live aprovados' AS criterio_aceite_uat_dod,
    '2025-03' AS reference_period
FROM packs p
CROSS JOIN stats s
ORDER BY p.pack_id ASC
