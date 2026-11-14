{{ config(materialized='table') }}

WITH packs AS (
    SELECT * FROM (
        VALUES
            ('P01', 'Modern Data Platform & Analytics (Lakehouse)'),
            ('P02', 'IA Agentica & Automacao LLM (MCP & REST)'),
            ('P03', 'Engenharia FullStack & Geolocalizacao Offline-First'),
            ('P04', 'Automacao Inteligente de Onboarding & Compliance KYB'),
            ('P05', 'FinOps & Series Macroeconomicas / Deflacionamento'),
            ('P06', 'Data Clean Rooms & Benchmarks Anonimizados'),
            ('P07', 'Due Diligence Corporativa, Sancoes & Contencioso (TPRM)'),
            ('P08', 'Seguranca de Pipelines & Auditoria de Vies/Qualidade'),
            ('P09', 'Inteligencia Industrial, Energia, Agro & Meio Ambiente'),
            ('P10', 'Observabilidade de Dados, Contratos ODCS & Confiabilidade SRE')
    ) AS t(pack_id, pack_name)
),

uf_features AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        COUNT(DISTINCT id_municipio) AS total_municipios_uf,
        ROUND(AVG(feat_score_propensao_b2b_composto), 2) AS avg_propensao_b2b_uf,
        ROUND(AVG(feat_score_risco_compliance_fiscal), 2) AS avg_risco_compliance_uf,
        ROUND(AVG(feat_score_conectividade_5g), 2) AS avg_conectividade_5g_uf,
        ROUND(AVG(feat_score_esg_agro_resiliencia), 2) AS avg_esg_agro_uf,
        ROUND(AVG(feat_score_governanca_datalake), 2) AS avg_governanca_datalake_uf,
        ROUND(AVG(feat_score_saude_assistencial), 2) AS avg_saude_uf,
        ROUND(AVG(feat_score_educacao_ideb), 2) AS avg_educacao_uf
    FROM {{ ref('feature_store_cross_domain_municipio') }}
    GROUP BY sigla_uf
),

uf_macro AS (
    SELECT
        sigla_uf,
        score_dinamismo_b2b_territorial,
        total_estabelecimentos_ativos,
        valor_homologado_compras_uf,
        total_sancoes_ativas_uf,
        macro_ano_mes_referencia
    FROM {{ ref('mart_cross_domain_territorio_b2b_macro_brasil') }}
)

SELECT
    p.pack_id,
    p.pack_name,
    u.sigla_uf,
    u.regiao,
    u.total_municipios_uf,
    COALESCE(m.score_dinamismo_b2b_territorial, 50.0) AS score_dinamismo_b2b_territorial,
    COALESCE(m.total_estabelecimentos_ativos, 0) AS total_estabelecimentos_ativos,
    COALESCE(m.valor_homologado_compras_uf, 0.0) AS valor_homologado_compras_uf,
    COALESCE(m.total_sancoes_ativas_uf, 0) AS total_sancoes_ativas_uf,
    CASE p.pack_id
        WHEN 'P01' THEN ROUND(0.50 * COALESCE(u.avg_governanca_datalake_uf, 50.0) + 0.50 * COALESCE(m.score_dinamismo_b2b_territorial, 50.0), 2)
        WHEN 'P02' THEN ROUND(0.60 * COALESCE(u.avg_governanca_datalake_uf, 50.0) + 0.40 * COALESCE(u.avg_conectividade_5g_uf, 50.0), 2)
        WHEN 'P03' THEN ROUND(0.65 * COALESCE(u.avg_conectividade_5g_uf, 50.0) + 0.35 * COALESCE(u.avg_propensao_b2b_uf, 50.0), 2)
        WHEN 'P04' THEN ROUND(0.60 * COALESCE(u.avg_propensao_b2b_uf, 50.0) + 0.40 * (100.0 - COALESCE(u.avg_risco_compliance_uf, 0.0)), 2)
        WHEN 'P05' THEN ROUND(0.70 * COALESCE(m.score_dinamismo_b2b_territorial, 50.0) + 0.30 * COALESCE(u.avg_propensao_b2b_uf, 50.0), 2)
        WHEN 'P06' THEN ROUND(0.55 * COALESCE(u.avg_propensao_b2b_uf, 50.0) + 0.45 * COALESCE(u.avg_governanca_datalake_uf, 50.0), 2)
        WHEN 'P07' THEN ROUND(0.55 * (100.0 - COALESCE(u.avg_risco_compliance_uf, 0.0)) + 0.45 * COALESCE(m.score_dinamismo_b2b_territorial, 50.0), 2)
        WHEN 'P08' THEN ROUND(0.50 * COALESCE(u.avg_governanca_datalake_uf, 50.0) + 0.50 * COALESCE(u.avg_educacao_uf, 50.0), 2)
        WHEN 'P09' THEN ROUND(0.75 * COALESCE(u.avg_esg_agro_uf, 50.0) + 0.25 * COALESCE(u.avg_conectividade_5g_uf, 50.0), 2)
        ELSE ROUND(0.60 * COALESCE(u.avg_governanca_datalake_uf, 50.0) + 0.40 * COALESCE(u.avg_saude_uf, 50.0), 2)
    END AS score_oportunidade_pack_uf,
    CASE
        WHEN p.pack_id IN ('P04', 'P07') AND COALESCE(m.total_sancoes_ativas_uf, 0) > 50 THEN 'ALTA_DEMANDA_COMPLIANCE_TPRM'
        WHEN p.pack_id = 'P09' AND COALESCE(u.avg_esg_agro_uf, 0.0) >= 60.0 THEN 'EXPANSAO_AGRO_ESG_RASTREAVEL'
        WHEN COALESCE(m.score_dinamismo_b2b_territorial, 50.0) >= 68.0 THEN 'ACELERACAO_COMERCIAL_PRIORITARIA'
        ELSE 'CONSOLIDACAO_ANALITICA_REGIONAL'
    END AS recomendacao_operacional_pack,
    COALESCE(m.macro_ano_mes_referencia, '2025-03') AS reference_period
FROM packs p
CROSS JOIN uf_features u
LEFT JOIN uf_macro m ON u.sigla_uf = m.sigla_uf
ORDER BY p.pack_id ASC, score_oportunidade_pack_uf DESC, u.sigla_uf ASC
