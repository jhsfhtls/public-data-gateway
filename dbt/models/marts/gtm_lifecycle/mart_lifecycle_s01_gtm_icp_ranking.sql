{{ config(materialized='table') }}

WITH packs AS (
    SELECT DISTINCT pack_id, pack_name
    FROM {{ ref('mart_solution_packs_insights_brasil') }}
),

mun AS (
    SELECT
        f.id_municipio,
        f.nome_municipio,
        f.sigla_uf,
        f.regiao,
        f.pib_per_capita_reais,
        f.populacao_residente,
        f.feat_score_propensao_b2b_composto,
        f.feat_score_risco_compliance_fiscal,
        f.feat_score_conectividade_5g,
        f.feat_score_esg_agro_resiliencia,
        f.feat_score_governanca_datalake,
        f.feat_score_saude_assistencial,
        f.feat_score_educacao_ideb,
        f.feat_score_dinamismo_b2b_uf,
        f.reference_date
    FROM {{ ref('feature_store_cross_domain_municipio') }} f
),

scored AS (
    SELECT
        p.pack_id,
        p.pack_name,
        m.id_municipio,
        m.nome_municipio,
        m.sigla_uf,
        m.regiao,
        m.populacao_residente,
        m.pib_per_capita_reais,
        CASE p.pack_id
            WHEN 'P01' THEN ROUND(0.55 * m.feat_score_propensao_b2b_composto + 0.45 * m.feat_score_governanca_datalake, 2)
            WHEN 'P02' THEN ROUND(0.55 * m.feat_score_governanca_datalake + 0.45 * m.feat_score_conectividade_5g, 2)
            WHEN 'P03' THEN ROUND(0.60 * m.feat_score_conectividade_5g + 0.40 * m.feat_score_propensao_b2b_composto, 2)
            WHEN 'P04' THEN ROUND(0.60 * m.feat_score_propensao_b2b_composto + 0.40 * (100.0 - m.feat_score_risco_compliance_fiscal), 2)
            WHEN 'P05' THEN ROUND(0.65 * m.feat_score_dinamismo_b2b_uf + 0.35 * m.feat_score_propensao_b2b_composto, 2)
            WHEN 'P06' THEN ROUND(0.55 * m.feat_score_propensao_b2b_composto + 0.45 * m.feat_score_governanca_datalake, 2)
            WHEN 'P07' THEN ROUND(0.55 * (100.0 - m.feat_score_risco_compliance_fiscal) + 0.45 * m.feat_score_dinamismo_b2b_uf, 2)
            WHEN 'P08' THEN ROUND(0.50 * m.feat_score_governanca_datalake + 0.50 * m.feat_score_educacao_ideb, 2)
            WHEN 'P09' THEN ROUND(0.70 * m.feat_score_esg_agro_resiliencia + 0.30 * m.feat_score_conectividade_5g, 2)
            ELSE ROUND(0.55 * m.feat_score_governanca_datalake + 0.45 * m.feat_score_saude_assistencial, 2)
        END AS score_icp_pack_municipio,
        m.reference_date
    FROM packs p
    CROSS JOIN mun m
)

SELECT
    pack_id,
    pack_name,
    id_municipio,
    nome_municipio,
    sigla_uf,
    regiao,
    populacao_residente,
    pib_per_capita_reais,
    score_icp_pack_municipio,
    CASE
        WHEN score_icp_pack_municipio >= 75.0 THEN 'TIER_1_ABM_PRIORITARIO'
        WHEN score_icp_pack_municipio >= 55.0 THEN 'TIER_2_OUTBOUND_QUALIFICADO'
        ELSE 'TIER_3_NURTURING_REGIONAL'
    END AS tier_icp_abm,
    CASE
        WHEN pack_id IN ('P01', 'P02') THEN 'Alta maturidade de dados e conectividade para Lakehouse e Agentes IA'
        WHEN pack_id IN ('P04', 'P07') THEN 'Forte densidade empresarial com necessidade de triagem KYB/TPRM'
        WHEN pack_id = 'P09' THEN 'Relevancia agroambiental/ESG com potencial de monitoramento geoespacial'
        ELSE 'Mercado regional com demanda de eficiencia operacional e analytics'
    END AS gancho_prospeccao_bant,
    reference_date
FROM scored
ORDER BY pack_id ASC, score_icp_pack_municipio DESC, id_municipio ASC
