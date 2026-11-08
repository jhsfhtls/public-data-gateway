{{ config(materialized='view') }}

SELECT
    simbolo_moeda,
    nome_moeda,
    tipo_moeda,
    reference_date,
    extracted_at
FROM {{ source('bcb_olinda_ptax', 'ptax_moedas') }}
