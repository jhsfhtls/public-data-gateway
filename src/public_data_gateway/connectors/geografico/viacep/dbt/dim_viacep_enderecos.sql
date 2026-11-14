{{ config(materialized='table') }}

SELECT
    cep,
    logradouro,
    bairro,
    localidade,
    sigla_uf,
    id_municipio,
    ddd,
    extracted_at
FROM {{ ref('stg_viacep_ceps') }}
