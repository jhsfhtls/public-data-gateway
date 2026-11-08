{{ config(materialized='view') }}

SELECT
    ispb,
    codigo_compensacao,
    nome_reduzido,
    nome_completo,
    cidade_sede,
    uf_sede,
    reference_date,
    extracted_at
FROM {{ source('brasilapi_core', 'brasilapi_bancos') }}
