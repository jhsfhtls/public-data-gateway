{{ config(materialized='table') }}

WITH uf_base AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        COUNT(*) AS total_municipios_uf,
        ROUND(AVG(feat_score_propensao_b2b_composto), 2) AS avg_propensao_b2b_uf
    FROM {{ ref('feature_store_cross_domain_municipio') }}
    GROUP BY sigla_uf
),
paises_iso AS (
    SELECT
        COUNT(*) AS total_paises_iso,
        MAX(population) AS max_populacao_iso
    FROM {{ ref('dim_paises_iso_3166') }}
),
openalex_summary AS (
    SELECT
        COUNT(*) AS total_obras_openalex,
        COALESCE(SUM(cited_by_count), 0) AS total_citacoes_openalex
    FROM {{ ref('fct_openalex_pesquisa_cientifica') }}
),
gdelt_summary AS (
    SELECT COUNT(*) AS total_artigos_gdelt
    FROM {{ ref('fct_gdelt_noticias_sentimento') }}
)
SELECT
    u.sigla_uf,
    u.regiao,
    u.total_municipios_uf,
    u.avg_propensao_b2b_uf,
    p.total_paises_iso,
    p.max_populacao_iso,
    o.total_obras_openalex,
    o.total_citacoes_openalex,
    g.total_artigos_gdelt
FROM uf_base u
CROSS JOIN paises_iso p
CROSS JOIN openalex_summary o
CROSS JOIN gdelt_summary g
