{{ config(materialized='table') }}

WITH base_mun AS (
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
    FROM {{ ref('stg_ibge_municipios') }}
)

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
    -- Conformed surrogate keys and analytics flags
    CASE
        WHEN sigla_uf IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO', 'MA', 'MT') THEN TRUE
        ELSE FALSE
    END AS is_amazonia_legal,
    CAST(reference_date AS DATE) AS valid_from,
    CAST(NULL AS DATE) AS valid_to,
    TRUE AS is_current,
    reference_date,
    extracted_at
FROM base_mun
