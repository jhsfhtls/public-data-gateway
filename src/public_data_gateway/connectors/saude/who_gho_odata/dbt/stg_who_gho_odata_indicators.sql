{{ config(materialized='view') }}

SELECT
    CAST(indicator_code AS VARCHAR) AS indicator_code,
    CAST(spatial_dim AS VARCHAR) AS spatial_dim,
    CAST(time_dim AS VARCHAR) AS time_dim,
    CAST(numeric_value AS DOUBLE) AS numeric_value,
    CAST(dim1 AS VARCHAR) AS dim1,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('who_gho_odata', 'who_gho_indicators') }}
WHERE indicator_code IS NOT NULL
  AND spatial_dim IS NOT NULL
  AND time_dim IS NOT NULL
  AND numeric_value IS NOT NULL
