{{ config(materialized='table') }}

WITH uf_risk AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        ROUND(AVG(feat_score_risco_compliance_fiscal), 2) AS indice_risco_compliance_uf,
        ROUND(AVG(feat_score_governanca_datalake), 2) AS indice_governanca_dados_uf
    FROM {{ ref('feature_store_cross_domain_municipio') }}
    GROUP BY sigla_uf
),

base AS (
    SELECT
        i.pack_id,
        i.pack_name,
        i.sigla_uf,
        i.regiao,
        i.total_sancoes_ativas_uf,
        COALESCE(m.valor_multas_ativas_uf, 0.0) AS valor_multas_ativas_uf,
        COALESCE(r.indice_risco_compliance_uf, 25.0) AS indice_risco_compliance_uf,
        COALESCE(r.indice_governanca_dados_uf, 50.0) AS indice_governanca_dados_uf,
        i.reference_period
    FROM {{ ref('mart_solution_packs_insights_brasil') }} i
    LEFT JOIN {{ ref('mart_cross_domain_territorio_b2b_macro_brasil') }} m
        ON i.sigla_uf = m.sigla_uf
    LEFT JOIN uf_risk r
        ON i.sigla_uf = r.sigla_uf
)

SELECT
    pack_id,
    pack_name,
    sigla_uf,
    regiao,
    total_sancoes_ativas_uf,
    valor_multas_ativas_uf,
    indice_risco_compliance_uf,
    indice_governanca_dados_uf,
    CASE
        WHEN total_sancoes_ativas_uf > 50 OR indice_risco_compliance_uf >= 50.0 THEN 'RIGOR_ALTO_ENHANCED_DD'
        WHEN total_sancoes_ativas_uf > 15 THEN 'RIGOR_MODERADO_PADRAO_KYB'
        ELSE 'RIGOR_PADRAO_SIMPLIFICADO'
    END AS nivel_rigor_due_diligence,
    CASE
        WHEN pack_id IN ('P02', 'P06', 'P08') THEN 'DPA com anonimizacao LGPD, guardrail anti-prompt-injection e Data Clean Room'
        WHEN pack_id IN ('P04', 'P07') THEN 'DPA com trilha de auditoria CEIS/CNEP/PEP e retencao probatoria'
        ELSE 'Kit padrao NDA + MSA + SOW + DPA com isolamento de dados publicos'
    END AS clausula_dpa_lgpd_recomendada,
    reference_period
FROM base
ORDER BY pack_id ASC, total_sancoes_ativas_uf DESC, sigla_uf ASC
