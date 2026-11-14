{{ config(materialized='table') }}

SELECT
    lei,
    legal_name,
    country,
    entity_status,
    extracted_at
FROM {{ ref('stg_gleif_lei_records') }}
