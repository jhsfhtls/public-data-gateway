{{ config(materialized='view') }}

SELECT
    codigo_qualificacao,
    descricao_qualificacao,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_qualificacoes') }}
