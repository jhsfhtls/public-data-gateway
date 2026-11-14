{{ config(materialized='table') }}

SELECT
    article_url,
    title,
    domain,
    language,
    source_country,
    seendate,
    extracted_at
FROM {{ ref('stg_gdelt_doc_articles') }}
