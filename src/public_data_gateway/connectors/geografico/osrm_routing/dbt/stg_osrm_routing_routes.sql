{{ config(materialized='view') }}

SELECT
    CAST(origin_municipio AS VARCHAR) AS origin_municipio,
    CAST(dest_municipio AS VARCHAR) AS dest_municipio,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(profile AS VARCHAR) AS profile,
    CAST(distance_km AS DOUBLE) AS distance_km,
    CAST(duration_min AS DOUBLE) AS duration_min,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('osrm_routing', 'osrm_routes') }}
WHERE origin_municipio IS NOT NULL
  AND dest_municipio IS NOT NULL
  AND distance_km IS NOT NULL
