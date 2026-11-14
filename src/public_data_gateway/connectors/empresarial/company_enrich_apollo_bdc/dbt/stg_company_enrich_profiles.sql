{{ config(materialized='view') }}

SELECT
    CAST(domain AS VARCHAR) AS domain,
    CAST(company_name AS VARCHAR) AS company_name,
    CAST(industry AS VARCHAR) AS industry,
    CAST(country_code AS VARCHAR) AS country_code,
    CAST(employee_range AS VARCHAR) AS employee_range,
    CAST(provider AS VARCHAR) AS provider,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('company_enrich_apollo_bdc', 'company_enrich_profiles') }}
