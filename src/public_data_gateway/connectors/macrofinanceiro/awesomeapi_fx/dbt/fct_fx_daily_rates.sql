{{ config(materialized='table') }}

SELECT
    pair_code,
    rate_date,
    code,
    codein,
    name,
    bid,
    ask,
    ROUND((ask - bid), 6) AS spread_bid_ask,
    pct_change,
    extracted_at
FROM {{ ref('stg_awesomeapi_fx_rates') }}
