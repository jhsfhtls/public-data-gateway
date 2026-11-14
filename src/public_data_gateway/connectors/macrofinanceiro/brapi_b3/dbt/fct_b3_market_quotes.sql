{{ config(materialized='table') }}

SELECT
    symbol,
    trade_date,
    short_name,
    price,
    change_pct,
    market_cap,
    extracted_at
FROM {{ ref('stg_brapi_b3_quotes') }}
