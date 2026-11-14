{{ config(materialized='view') }}

SELECT
    CAST(id_municipio AS VARCHAR) AS id_municipio,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(obs_date AS VARCHAR) AS obs_date,
    CAST(solar_radiation_kwh_m2 AS DOUBLE) AS solar_radiation_kwh_m2,
    CAST(precipitation_mm AS DOUBLE) AS precipitation_mm,
    CAST(temp_2m_c AS DOUBLE) AS temp_2m_c,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('nasa_power', 'nasa_power_daily') }}
WHERE id_municipio IS NOT NULL
  AND obs_date IS NOT NULL
