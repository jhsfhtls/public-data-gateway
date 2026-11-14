{{ config(materialized='table') }}

SELECT
    id_municipio,
    sigla_uf,
    lat,
    lon,
    elevation_m,
    extracted_at
FROM {{ ref('stg_open_elevation_points') }}
