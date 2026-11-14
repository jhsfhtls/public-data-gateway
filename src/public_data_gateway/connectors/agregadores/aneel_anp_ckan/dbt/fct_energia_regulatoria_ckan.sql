{{ config(materialized='table') }}

SELECT
    portal,
    resource_id,
    record_id,
    reference_date,
    metric_name,
    metric_value,
    uf,
    extracted_at
FROM {{ ref('stg_aneel_anp_ckan_records') }}
