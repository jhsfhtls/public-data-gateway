{{ config(materialized='table') }}

SELECT
    feature_group_id,
    provider,
    domain,
    coverage_scope,
    status,
    extracted_at
FROM {{ ref('stg_upgini_adapter_features') }}
