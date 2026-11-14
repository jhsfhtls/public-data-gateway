{{ config(materialized='view') }}

SELECT
    CAST(domain AS VARCHAR) AS domain,
    CAST(status_list AS VARCHAR) AS status_list,
    CAST(registered_at AS VARCHAR) AS registered_at,
    CAST(last_changed_at AS VARCHAR) AS last_changed_at,
    CAST(nameserver_count AS INTEGER) AS nameserver_count,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('rdap_registro_br', 'rdap_registro_br_domains') }}
