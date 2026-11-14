{{ config(materialized='table') }}

SELECT
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    ROUND(close_price - open_price, 4) AS intraday_return_abs,
    volume,
    extracted_at
FROM {{ ref('stg_alpha_vantage_daily') }}
