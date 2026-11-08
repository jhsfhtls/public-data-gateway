{{ config(materialized='view') }}

SELECT
    cnpj_14,
    codigo_cvm,
    nome_social,
    nome_comercial,
    status,
    municipio,
    sigla_uf,
    cep_8,
    reference_date,
    extracted_at
FROM {{ source('brasilapi_core', 'brasilapi_corretoras') }}
