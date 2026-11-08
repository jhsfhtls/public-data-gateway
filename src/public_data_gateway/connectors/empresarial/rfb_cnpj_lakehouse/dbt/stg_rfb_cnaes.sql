{{ config(materialized='view') }}

SELECT
    codigo_cnae,
    descricao_cnae,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_cnaes') }}
