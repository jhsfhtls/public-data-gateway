{{ config(materialized='view') }}

SELECT
    id_regiao,
    sigla_regiao,
    nome_regiao,
    reference_date,
    extracted_at
FROM {{ source('ibge_localidades', 'regioes') }}
