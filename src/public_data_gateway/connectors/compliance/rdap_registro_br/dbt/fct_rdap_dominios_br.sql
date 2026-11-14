{{ config(materialized='table') }}

SELECT
    domain,
    status_list,
    registered_at,
    last_changed_at,
    nameserver_count,
    extracted_at
FROM {{ ref('stg_rdap_registro_br_domains') }}
