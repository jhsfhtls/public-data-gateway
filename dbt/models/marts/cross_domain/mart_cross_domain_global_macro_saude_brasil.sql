{{ config(materialized='table') }}

WITH uf_features AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        COUNT(*) AS total_municipios_uf,
        ROUND(AVG(feat_score_propensao_b2b_composto), 2) AS avg_propensao_b2b_uf,
        ROUND(AVG(feat_score_risco_compliance_fiscal), 2) AS avg_risco_compliance_uf
    FROM {{ ref('feature_store_cross_domain_municipio') }}
    GROUP BY sigla_uf
),
ipea_latest AS (
    SELECT
        MAX(valor) AS ipeadata_ref_valor
    FROM {{ ref('fct_ipeadata_macro_series') }}
),
b3_summary AS (
    SELECT
        COUNT(DISTINCT symbol) AS b3_tickers_monitorados,
        ROUND(AVG(price), 2) AS b3_preco_medio_brl
    FROM {{ ref('fct_b3_market_quotes') }}
),
fx_summary AS (
    SELECT
        MAX(CASE WHEN pair_code = 'USDBRL' THEN bid END) AS usd_brl_bid,
        MAX(CASE WHEN pair_code = 'EURBRL' THEN bid END) AS eur_brl_bid
    FROM {{ ref('fct_fx_daily_rates') }}
),
energia_uf AS (
    SELECT
        uf AS sigla_uf,
        COUNT(*) AS total_registros_energia_ckan,
        ROUND(AVG(metric_value), 4) AS avg_metric_energia_ckan
    FROM {{ ref('fct_energia_regulatoria_ckan') }}
    GROUP BY uf
),
parlamentares_uf AS (
    SELECT
        sigla_uf,
        COUNT(DISTINCT id_deputado) AS total_deputados_ativos_uf
    FROM {{ ref('dim_parlamentares_federais') }}
    GROUP BY sigla_uf
),
wb_brasil AS (
    SELECT
        MAX(value) AS world_bank_gdp_growth_pct
    FROM {{ ref('fct_world_bank_macro_brasil') }}
    WHERE country_iso3 = 'BRA'
),
who_brasil AS (
    SELECT
        MAX(numeric_value) AS who_life_expectancy_years
    FROM {{ ref('fct_who_gho_saude_brasil') }}
    WHERE spatial_dim = 'BRA'
)
SELECT
    u.sigla_uf,
    u.regiao,
    u.total_municipios_uf,
    u.avg_propensao_b2b_uf,
    u.avg_risco_compliance_uf,
    COALESCE(e.total_registros_energia_ckan, 0) AS total_registros_energia_ckan,
    e.avg_metric_energia_ckan,
    COALESCE(p.total_deputados_ativos_uf, 0) AS total_deputados_ativos_uf,
    i.ipeadata_ref_valor,
    b.b3_tickers_monitorados,
    b.b3_preco_medio_brl,
    f.usd_brl_bid,
    f.eur_brl_bid,
    w.world_bank_gdp_growth_pct,
    h.who_life_expectancy_years
FROM uf_features u
LEFT JOIN energia_uf e ON u.sigla_uf = e.sigla_uf
LEFT JOIN parlamentares_uf p ON u.sigla_uf = p.sigla_uf
CROSS JOIN ipea_latest i
CROSS JOIN b3_summary b
CROSS JOIN fx_summary f
CROSS JOIN wb_brasil w
CROSS JOIN who_brasil h
