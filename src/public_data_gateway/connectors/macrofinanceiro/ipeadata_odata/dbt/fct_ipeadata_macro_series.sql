{{ config(materialized='table') }}

SELECT
    sercodigo,
    data_referencia,
    LEFT(data_referencia, 7) AS ano_mes,
    valor,
    tercodigo,
    nivnome,
    extracted_at
FROM {{ ref('stg_ipeadata_odata_series') }}
