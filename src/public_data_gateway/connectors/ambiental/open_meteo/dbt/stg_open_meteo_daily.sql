{{ config(materialized='view') }}

SELECT
    CAST(id_municipio AS VARCHAR) AS id_municipio,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(forecast_date AS VARCHAR) AS forecast_date,
    CAST(temp_max_c AS DOUBLE) AS temp_max_c,
    CAST(temp_min_c AS DOUBLE) AS temp_min_c,
    CAST(precipitation_mm AS DOUBLE) AS precipitation_mm,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('open_meteo', 'open_meteo_daily') }}
WHERE id_municipio IS NOT NULL
  AND forecast_date IS NOT NULL
