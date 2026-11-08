{{ config(materialized='view') }}

SELECT
    codigo_pais,
    nome_pais,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_paises') }}
