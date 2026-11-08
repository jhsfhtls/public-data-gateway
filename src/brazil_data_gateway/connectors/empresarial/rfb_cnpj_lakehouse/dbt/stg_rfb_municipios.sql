{{ config(materialized='view') }}

SELECT
    codigo_municipio_rf,
    nome_municipio,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_municipios') }}
