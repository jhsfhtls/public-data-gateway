{{ config(materialized='view') }}

SELECT
    cep_8,
    sigla_uf,
    nome_municipio,
    bairro,
    logradouro,
    id_municipio,
    latitude,
    longitude,
    reference_date,
    extracted_at
FROM {{ source('brasilapi_core', 'brasilapi_ceps') }}
