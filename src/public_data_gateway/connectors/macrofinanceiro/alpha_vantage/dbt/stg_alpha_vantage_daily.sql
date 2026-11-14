{{ config(materialized='view') }}

SELECT
    CAST(symbol AS VARCHAR) AS symbol,
    CAST(trade_date AS VARCHAR) AS trade_date,
    CAST(open_price AS DOUBLE) AS open_price,
    CAST(high_price AS DOUBLE) AS high_price,
    CAST(low_price AS DOUBLE) AS low_price,
    CAST(close_price AS DOUBLE) AS close_price,
    CAST(volume AS DOUBLE) AS volume,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('alpha_vantage', 'alpha_vantage_daily') }}
WHERE symbol IS NOT NULL
  AND trade_date IS NOT NULL
