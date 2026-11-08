{{ config(materialized='view') }}

SELECT
    ispb,
    nome,
    nome_reduzido,
    modalidade_participacao,
    tipo_participacao,
    inicio_operacao,
    reference_date,
    extracted_at
FROM {{ source('brasilapi_core', 'brasilapi_pix_participantes') }}
