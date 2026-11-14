{{ config(materialized='table') }}

SELECT
    country_iso3,
    indicator_id,
    year,
    value,
    extracted_at
FROM {{ ref('stg_world_bank_v2_indicators') }}
