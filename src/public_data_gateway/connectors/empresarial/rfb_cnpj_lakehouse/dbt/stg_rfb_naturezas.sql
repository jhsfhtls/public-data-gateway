{{ config(materialized='view') }}

SELECT
    codigo_natureza,
    descricao_natureza,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_naturezas') }}
