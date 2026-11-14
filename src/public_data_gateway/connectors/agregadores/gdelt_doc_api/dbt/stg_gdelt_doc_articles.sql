{{ config(materialized='view') }}

SELECT
    CAST(article_url AS VARCHAR) AS article_url,
    CAST(title AS VARCHAR) AS title,
    CAST(domain AS VARCHAR) AS domain,
    CAST(language AS VARCHAR) AS language,
    CAST(source_country AS VARCHAR) AS source_country,
    CAST(seendate AS VARCHAR) AS seendate,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('gdelt_doc_api', 'gdelt_doc_articles') }}
