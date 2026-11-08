{{ config(materialized='view') }}

SELECT
    id_distrito,
    nome_distrito,
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    sigla_regiao,
    reference_date,
    extracted_at
FROM {{ source('ibge_localidades', 'distritos') }}
