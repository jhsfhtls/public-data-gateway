{{ config(materialized='table') }}

SELECT
    query_key,
    display_name,
    lat,
    lon,
    place_type,
    sigla_uf,
    extracted_at
FROM {{ ref('stg_nominatim_osm_geocodes') }}
