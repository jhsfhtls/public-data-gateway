{{ config(materialized='table') }}

SELECT
    id_municipio,
    sigla_uf,
    forecast_date,
    temp_max_c,
    temp_min_c,
    ROUND(temp_max_c - temp_min_c, 2) AS amplitude_termica_c,
    precipitation_mm,
    extracted_at
FROM {{ ref('stg_open_meteo_daily') }}
