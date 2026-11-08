{{ config(materialized='table') }}

WITH base_moedas AS (
    SELECT
        simbolo_moeda,
        nome_moeda,
        tipo_moeda,
        CAST(reference_date AS DATE) AS reference_date,
        extracted_at
    FROM {{ ref('stg_bcb_ptax_moedas') }}
)

SELECT
    simbolo_moeda,
    nome_moeda,
    tipo_moeda,
    reference_date,
    extracted_at
FROM base_moedas
