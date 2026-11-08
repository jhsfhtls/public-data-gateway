{{ config(materialized='view') }}

SELECT
    ddd,
    sigla_uf,
    nome_municipio,
    reference_date,
    extracted_at
FROM {{ source('brasilapi_core', 'brasilapi_ddds') }}
