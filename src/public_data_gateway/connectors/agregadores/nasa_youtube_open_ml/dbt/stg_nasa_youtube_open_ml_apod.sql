{{ config(materialized='view') }}

SELECT
    CAST(apod_date AS VARCHAR) AS apod_date,
    CAST(title AS VARCHAR) AS title,
    CAST(media_type AS VARCHAR) AS media_type,
    CAST(url AS VARCHAR) AS url,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('nasa_youtube_open_ml', 'nasa_apod_catalog') }}
WHERE apod_date IS NOT NULL
