{{ config(materialized='view') }}

SELECT
    CAST(ip AS VARCHAR) AS ip,
    CAST(ptr_record AS VARCHAR) AS ptr_record,
    CAST(asn AS INTEGER) AS asn,
    CAST(asn_name AS VARCHAR) AS asn_name,
    CAST(country_code AS VARCHAR) AS country_code,
    CAST(provider_used AS VARCHAR) AS provider_used,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('bgpview_ip_intel', 'bgpview_ip_intel_ips') }}
