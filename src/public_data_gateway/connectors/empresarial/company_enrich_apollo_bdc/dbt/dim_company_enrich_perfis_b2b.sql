{{ config(materialized='table') }}

SELECT
    domain,
    company_name,
    industry,
    country_code,
    employee_range,
    provider,
    extracted_at
FROM {{ ref('stg_company_enrich_profiles') }}
