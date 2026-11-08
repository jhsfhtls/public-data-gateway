{{ config(materialized='table') }}

WITH mun AS (
    SELECT * FROM {{ ref('dim_municipio') }}
),

dist_counts AS (
    SELECT
        id_municipio,
        COUNT(DISTINCT id_distrito) AS qtd_distritos_no_municipio
    FROM {{ ref('stg_ibge_distritos') }}
    GROUP BY id_municipio
),

subdist_counts AS (
    SELECT
        id_municipio,
        COUNT(DISTINCT id_subdistrito) AS qtd_subdistritos_no_municipio
    FROM {{ ref('stg_ibge_subdistritos') }}
    GROUP BY id_municipio
),

uf_counts AS (
    SELECT
        sigla_uf,
        COUNT(DISTINCT id_municipio) AS qtd_municipios_na_uf
    FROM mun
    GROUP BY sigla_uf
),

regiao_counts AS (
    SELECT
        regiao,
        COUNT(DISTINCT id_municipio) AS qtd_municipios_na_regiao
    FROM mun
    GROUP BY regiao
)

SELECT
    m.id_municipio,
    m.id_municipio_6,
    m.nome_municipio,
    m.sigla_uf,
    m.id_uf,
    m.nome_uf,
    m.regiao,
    m.sigla_regiao,
    m.mesorregiao,
    m.microrregiao,
    m.regiao_intermediaria,
    m.regiao_imediata,
    m.is_capital,
    m.is_amazonia_legal,
    COALESCE(dc.qtd_distritos_no_municipio, 1) AS qtd_distritos_no_municipio,
    COALESCE(sc.qtd_subdistritos_no_municipio, 0) AS qtd_subdistritos_no_municipio,
    uc.qtd_municipios_na_uf,
    rc.qtd_municipios_na_regiao,
    m.reference_date,
    m.extracted_at
FROM mun m
LEFT JOIN dist_counts dc ON m.id_municipio = dc.id_municipio
LEFT JOIN subdist_counts sc ON m.id_municipio = sc.id_municipio
LEFT JOIN uf_counts uc ON m.sigla_uf = uc.sigla_uf
LEFT JOIN regiao_counts rc ON m.regiao = rc.regiao
