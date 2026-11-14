{{ config(materialized='view') }}

SELECT
    CAST(feature_group_id AS VARCHAR) AS feature_group_id,
    CAST(provider AS VARCHAR) AS provider,
    CAST(domain AS VARCHAR) AS domain,
    CAST(coverage_scope AS VARCHAR) AS coverage_scope,
    CAST(status AS VARCHAR) AS status,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('upgini_adapter', 'upgini_feature_catalog') }}
WHERE feature_group_id IS NOT NULL
