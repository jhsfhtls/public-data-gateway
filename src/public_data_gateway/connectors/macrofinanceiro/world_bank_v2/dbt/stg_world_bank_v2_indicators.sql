{{ config(materialized='view') }}

SELECT
    CAST(country_iso3 AS VARCHAR) AS country_iso3,
    CAST(indicator_id AS VARCHAR) AS indicator_id,
    CAST(year AS VARCHAR) AS year,
    CAST(value AS DOUBLE) AS value,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('world_bank_v2', 'world_bank_indicators') }}
WHERE country_iso3 IS NOT NULL
  AND indicator_id IS NOT NULL
  AND year IS NOT NULL
  AND value IS NOT NULL
