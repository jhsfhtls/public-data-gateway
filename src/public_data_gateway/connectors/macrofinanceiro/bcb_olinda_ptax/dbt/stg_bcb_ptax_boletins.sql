{{ config(materialized='view') }}

SELECT
    simbolo_moeda,
    data_cotacao,
    data_hora_cotacao,
    ano_mes,
    cotacao_compra,
    cotacao_venda,
    paridade_compra,
    paridade_venda,
    tipo_boletim,
    reference_date,
    extracted_at
FROM {{ source('bcb_olinda_ptax', 'ptax_boletins') }}
