{{ config(materialized='view') }}

SELECT
    CAST(pair_code AS VARCHAR) AS pair_code,
    CAST(rate_date AS VARCHAR) AS rate_date,
    CAST(code AS VARCHAR) AS code,
    CAST(codein AS VARCHAR) AS codein,
    CAST(name AS VARCHAR) AS name,
    CAST(bid AS DOUBLE) AS bid,
    CAST(ask AS DOUBLE) AS ask,
    CAST(pct_change AS DOUBLE) AS pct_change,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('awesomeapi_fx', 'awesomeapi_rates') }}
WHERE pair_code IS NOT NULL
  AND rate_date IS NOT NULL
  AND bid IS NOT NULL
