{{ config(materialized='table') }}

WITH api_blocks AS (
    SELECT * FROM (
        VALUES
            ('BLOCO_1_KYC_GEO', 'Enriquecimento Cadastral, Geolocalizacao, KYC/KYB & Antifraude', 10, 9, 'P04, P07, P03', 'FREE_CASCADE_AND_FREEMIUM'),
            ('BLOCO_2_MERCADO_REGULATORIO', 'Inteligencia de Mercado, Regulatorias, Economia & Governo', 7, 7, 'P05, P01, P07, P09', 'FREE_OPEN_DATA'),
            ('BLOCO_3_ESPACIAL_CLIMA_ML', 'Feature Engineering Espacial, Mobilidade, Clima & ML', 9, 9, 'P03, P01, P02, P09', 'FREE_AND_ENV_KEY_GATED'),
            ('BLOCO_4_GLOBAL_PESQUISA', 'Dados Globais, Demografia, Pesquisa & Sentimento', 3, 3, 'P01, P02, P06, P07', 'FREE_OPEN_DATA')
    ) AS t(block_id, block_name, total_curated_apis, homologated_apis, target_packs, finops_profile)
),

uf_base AS (
    SELECT
        sigla_uf,
        regiao,
        ROUND(AVG(feat_score_propensao_b2b_composto), 2) AS avg_propensao_b2b_uf,
        ROUND(AVG(feat_score_risco_compliance_fiscal), 2) AS avg_risco_compliance_uf,
        ROUND(AVG(feat_score_conectividade_5g), 2) AS avg_conectividade_5g_uf
    FROM {{ ref('feature_store_cross_domain_municipio') }}
    GROUP BY sigla_uf, regiao
)

SELECT
    b.block_id,
    b.block_name,
    b.total_curated_apis,
    b.homologated_apis,
    ROUND(100.0 * b.homologated_apis / NULLIF(b.total_curated_apis, 0), 2) AS pct_homologated,
    b.target_packs,
    b.finops_profile,
    u.sigla_uf,
    u.regiao,
    u.avg_propensao_b2b_uf,
    u.avg_risco_compliance_uf,
    u.avg_conectividade_5g_uf,
    CASE b.block_id
        WHEN 'BLOCO_1_KYC_GEO' THEN ROUND(0.55 * u.avg_propensao_b2b_uf + 0.45 * u.avg_risco_compliance_uf, 2)
        WHEN 'BLOCO_2_MERCADO_REGULATORIO' THEN ROUND(0.60 * u.avg_propensao_b2b_uf + 0.40 * u.avg_conectividade_5g_uf, 2)
        WHEN 'BLOCO_3_ESPACIAL_CLIMA_ML' THEN ROUND(0.60 * u.avg_conectividade_5g_uf + 0.40 * u.avg_propensao_b2b_uf, 2)
        ELSE ROUND(0.50 * u.avg_propensao_b2b_uf + 0.50 * u.avg_conectividade_5g_uf, 2)
    END AS score_prontidao_ml_uf
FROM api_blocks b
CROSS JOIN uf_base u
