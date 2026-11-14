{{ config(materialized='table') }}

SELECT
    cep,
    logradouro,
    bairro,
    localidade,
    sigla_uf,
    id_municipio,
    extracted_at
FROM {{ ref('stg_opencep_ceps') }}
