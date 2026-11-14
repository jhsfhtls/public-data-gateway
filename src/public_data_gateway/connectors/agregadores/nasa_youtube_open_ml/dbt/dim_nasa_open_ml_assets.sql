{{ config(materialized='table') }}

SELECT
    apod_date,
    title,
    media_type,
    url,
    extracted_at
FROM {{ ref('stg_nasa_youtube_open_ml_apod') }}
