{{ config(materialized='view') }}

SELECT
    CAST(portal AS VARCHAR) AS portal,
    CAST(resource_id AS VARCHAR) AS resource_id,
    CAST(record_id AS VARCHAR) AS record_id,
    CAST(reference_date AS VARCHAR) AS reference_date,
    CAST(metric_name AS VARCHAR) AS metric_name,
    CAST(metric_value AS DOUBLE) AS metric_value,
    CAST(uf AS VARCHAR) AS uf,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('aneel_anp_ckan', 'ckan_energy_records') }}
WHERE portal IS NOT NULL
  AND resource_id IS NOT NULL
  AND record_id IS NOT NULL
