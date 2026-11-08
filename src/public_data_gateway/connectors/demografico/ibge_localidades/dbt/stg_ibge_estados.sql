{{ config(materialized='view') }}

SELECT
    id_uf,
    sigla_uf,
    nome_uf,
    regiao,
    sigla_regiao,
    id_regiao,
    reference_date,
    extracted_at
FROM {{ source('ibge_localidades', 'estados') }}
