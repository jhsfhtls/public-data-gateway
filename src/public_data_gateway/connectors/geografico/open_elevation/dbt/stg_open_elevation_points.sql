{{ config(materialized='view') }}

SELECT
    CAST(id_municipio AS VARCHAR) AS id_municipio,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lon AS DOUBLE) AS lon,
    CAST(elevation_m AS DOUBLE) AS elevation_m,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('open_elevation', 'open_elevation_points') }}
WHERE id_municipio IS NOT NULL
  AND elevation_m IS NOT NULL
