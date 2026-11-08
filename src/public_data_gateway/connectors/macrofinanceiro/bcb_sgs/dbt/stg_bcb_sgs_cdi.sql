{{ config(materialized='view') }}

SELECT
    id_serie,
    data_referencia,
    ano_mes,
    valor,
    nome_serie,
    unidade_medida,
    periodicidade,
    reference_date,
    extracted_at
FROM {{ source('bcb_sgs', 'sgs_cdi') }}
