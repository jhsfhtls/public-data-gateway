{{ config(materialized='table') }}

SELECT
    work_id,
    title,
    publication_year,
    cited_by_count,
    doi,
    extracted_at
FROM {{ ref('stg_openalex_works') }}
