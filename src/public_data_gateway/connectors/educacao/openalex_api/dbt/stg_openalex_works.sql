{{ config(materialized='view') }}

SELECT
    CAST(work_id AS VARCHAR) AS work_id,
    CAST(title AS VARCHAR) AS title,
    CAST(publication_year AS INTEGER) AS publication_year,
    CAST(cited_by_count AS INTEGER) AS cited_by_count,
    CAST(doi AS VARCHAR) AS doi,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('openalex_api', 'openalex_works') }}
