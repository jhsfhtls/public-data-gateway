{{ config(materialized='table') }}

SELECT
    ip,
    ptr_record,
    asn,
    asn_name,
    country_code,
    provider_used,
    extracted_at
FROM {{ ref('stg_bgpview_ip_intel_ips') }}
