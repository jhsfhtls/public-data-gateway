{{ config(materialized='view') }}

SELECT
    CAST(query_key AS VARCHAR) AS query_key,
    CAST(display_name AS VARCHAR) AS display_name,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lon AS DOUBLE) AS lon,
    CAST(place_type AS VARCHAR) AS place_type,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('nominatim_osm', 'nominatim_osm_geocodes') }}
