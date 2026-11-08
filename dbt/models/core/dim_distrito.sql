{{ config(materialized='table') }}

WITH dist AS (
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
    FROM {{ ref('stg_ibge_distritos') }}
)

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
    -- Flag if district is the municipal seat (same name as municipality)
    CASE
        WHEN LOWER(TRIM(nome_distrito)) = LOWER(TRIM(nome_municipio)) THEN TRUE
        ELSE FALSE
    END AS is_distrito_sede,
    reference_date,
    extracted_at
FROM dist
