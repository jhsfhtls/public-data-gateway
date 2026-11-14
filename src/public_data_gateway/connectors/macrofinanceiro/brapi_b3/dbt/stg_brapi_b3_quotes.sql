{{ config(materialized='view') }}

SELECT
    CAST(symbol AS VARCHAR) AS symbol,
    CAST(trade_date AS VARCHAR) AS trade_date,
    CAST(short_name AS VARCHAR) AS short_name,
    CAST(price AS DOUBLE) AS price,
    CAST(change_pct AS DOUBLE) AS change_pct,
    CAST(market_cap AS DOUBLE) AS market_cap,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('brapi_b3', 'brapi_quotes') }}
WHERE symbol IS NOT NULL
  AND trade_date IS NOT NULL
  AND price IS NOT NULL
