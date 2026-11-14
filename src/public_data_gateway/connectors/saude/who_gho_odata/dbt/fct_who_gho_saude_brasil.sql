{{ config(materialized='table') }}

SELECT
    indicator_code,
    spatial_dim,
    time_dim,
    numeric_value,
    dim1,
    extracted_at
FROM {{ ref('stg_who_gho_odata_indicators') }}
