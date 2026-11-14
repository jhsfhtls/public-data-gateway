{{ config(materialized='table') }}

SELECT
    id_municipio,
    sigla_uf,
    obs_date,
    solar_radiation_kwh_m2,
    precipitation_mm,
    temp_2m_c,
    extracted_at
FROM {{ ref('stg_nasa_power_daily') }}
