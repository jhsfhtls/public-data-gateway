{{ config(materialized='view') }}

SELECT
    CAST(id_municipio AS VARCHAR) AS id_municipio,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lon AS DOUBLE) AS lon,
    CAST(amenity AS VARCHAR) AS amenity,
    CAST(radius_m AS INTEGER) AS radius_m,
    CAST(total_pois AS DOUBLE) AS total_pois,
    CAST(nodes AS INTEGER) AS nodes,
    CAST(ways AS INTEGER) AS ways,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('overpass_osm', 'osm_poi_counts') }}
WHERE id_municipio IS NOT NULL
  AND amenity IS NOT NULL
