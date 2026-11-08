{{ config(materialized='view') }}

SELECT
    id_municipio,
    id_municipio_6,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    sigla_regiao,
    mesorregiao,
    microrregiao,
    regiao_intermediaria,
    regiao_imediata,
    is_capital,
    reference_date,
    extracted_at
FROM {{ source('ibge_localidades', 'municipios') }}
