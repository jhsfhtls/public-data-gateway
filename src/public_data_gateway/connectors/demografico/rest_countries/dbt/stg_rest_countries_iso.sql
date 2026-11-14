{{ config(materialized='view') }}

SELECT
    CAST(cca2 AS VARCHAR) AS cca2,
    CAST(cca3 AS VARCHAR) AS cca3,
    CAST(common_name AS VARCHAR) AS common_name,
    CAST(region AS VARCHAR) AS region,
    CAST(subregion AS VARCHAR) AS subregion,
    CAST(population AS BIGINT) AS population,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('rest_countries', 'rest_countries_iso') }}
