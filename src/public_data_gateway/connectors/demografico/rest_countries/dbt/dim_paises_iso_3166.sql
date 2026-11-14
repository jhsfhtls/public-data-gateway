{{ config(materialized='table') }}

SELECT
    cca2,
    cca3,
    common_name,
    region,
    subregion,
    population,
    extracted_at
FROM {{ ref('stg_rest_countries_iso') }}
