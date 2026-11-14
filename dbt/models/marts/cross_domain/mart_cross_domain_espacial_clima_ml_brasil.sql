{{ config(materialized='table') }}

WITH uf_features AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        COUNT(*) AS total_municipios_uf,
        ROUND(AVG(feat_score_propensao_b2b_composto), 2) AS avg_propensao_b2b_uf,
        ROUND(AVG(feat_score_esg_agro_resiliencia), 2) AS avg_esg_agro_uf
    FROM {{ ref('feature_store_cross_domain_municipio') }}
    GROUP BY sigla_uf
),
osm_uf AS (
    SELECT
        sigla_uf,
        SUM(total_pois) AS total_osm_pois_uf
    FROM {{ ref('fct_osm_pois_municipio') }}
    GROUP BY sigla_uf
),
osrm_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(distance_km), 2) AS avg_osrm_distance_km
    FROM {{ ref('fct_osrm_rotas_logisticas') }}
    GROUP BY sigla_uf
),
meteo_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(temp_max_c), 2) AS avg_temp_max_c,
        ROUND(AVG(precipitation_mm), 2) AS avg_meteo_precip_mm
    FROM {{ ref('fct_open_meteo_clima_municipio') }}
    GROUP BY sigla_uf
),
nasa_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(solar_radiation_kwh_m2), 2) AS avg_solar_radiation_kwh_m2
    FROM {{ ref('fct_nasa_power_agro_clima') }}
    GROUP BY sigla_uf
),
elev_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(elevation_m), 1) AS avg_elevation_m
    FROM {{ ref('dim_elevacao_terreno_municipio') }}
    GROUP BY sigla_uf
),
hf_summary AS (
    SELECT
        COUNT(*) AS total_hf_signals,
        ROUND(AVG(top_score), 4) AS avg_hf_confidence
    FROM {{ ref('fct_huggingface_nlp_signals') }}
),
av_summary AS (
    SELECT
        COUNT(DISTINCT symbol) AS alpha_vantage_tickers,
        ROUND(AVG(close_price), 2) AS avg_alpha_vantage_close
    FROM {{ ref('fct_alpha_vantage_series') }}
),
nasa_assets AS (
    SELECT COUNT(*) AS total_nasa_ml_assets
    FROM {{ ref('dim_nasa_open_ml_assets') }}
),
upgini_groups AS (
    SELECT COUNT(*) AS total_upgini_feature_groups
    FROM {{ ref('dim_upgini_feature_groups') }}
)
SELECT
    u.sigla_uf,
    u.regiao,
    u.total_municipios_uf,
    u.avg_propensao_b2b_uf,
    u.avg_esg_agro_uf,
    COALESCE(o.total_osm_pois_uf, 0) AS total_osm_pois_uf,
    r.avg_osrm_distance_km,
    m.avg_temp_max_c,
    m.avg_meteo_precip_mm,
    n.avg_solar_radiation_kwh_m2,
    e.avg_elevation_m,
    hf.total_hf_signals,
    hf.avg_hf_confidence,
    av.alpha_vantage_tickers,
    av.avg_alpha_vantage_close,
    na.total_nasa_ml_assets,
    ug.total_upgini_feature_groups
FROM uf_features u
LEFT JOIN osm_uf o ON u.sigla_uf = o.sigla_uf
LEFT JOIN osrm_uf r ON u.sigla_uf = r.sigla_uf
LEFT JOIN meteo_uf m ON u.sigla_uf = m.sigla_uf
LEFT JOIN nasa_uf n ON u.sigla_uf = n.sigla_uf
LEFT JOIN elev_uf e ON u.sigla_uf = e.sigla_uf
CROSS JOIN hf_summary hf
CROSS JOIN av_summary av
CROSS JOIN nasa_assets na
CROSS JOIN upgini_groups ug
