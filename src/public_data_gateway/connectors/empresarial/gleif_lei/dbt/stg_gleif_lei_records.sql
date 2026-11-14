{{ config(materialized='view') }}

SELECT
    CAST(lei AS VARCHAR) AS lei,
    CAST(legal_name AS VARCHAR) AS legal_name,
    CAST(country AS VARCHAR) AS country,
    CAST(entity_status AS VARCHAR) AS entity_status,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('gleif_lei', 'gleif_lei_records') }}
