{{ config(materialized='table') }}

SELECT
    id_municipio,
    sigla_uf,
    lat,
    lon,
    amenity,
    radius_m,
    total_pois,
    nodes,
    ways,
    extracted_at
FROM {{ ref('stg_overpass_osm_pois') }}
