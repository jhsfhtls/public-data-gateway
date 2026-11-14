{{ config(materialized='table') }}

SELECT
    origin_municipio,
    dest_municipio,
    sigla_uf,
    profile,
    distance_km,
    duration_min,
    ROUND(distance_km / NULLIF(duration_min / 60.0, 0), 2) AS avg_speed_kmh,
    extracted_at
FROM {{ ref('stg_osrm_routing_routes') }}
