{{ config(materialized='table') }}

WITH base AS (
    SELECT
        i.pack_id,
        i.pack_name,
        i.sigla_uf,
        i.regiao,
        i.score_oportunidade_pack_uf,
        i.recomendacao_operacional_pack,
        i.reference_period
    FROM {{ ref('mart_solution_packs_insights_brasil') }} i
)

SELECT
    pack_id,
    pack_name,
    sigla_uf,
    regiao,
    score_oportunidade_pack_uf,
    CASE pack_id
        WHEN 'P01' THEN 'P02'
        WHEN 'P02' THEN 'P08'
        WHEN 'P03' THEN 'P01'
        WHEN 'P04' THEN 'P07'
        WHEN 'P05' THEN 'P01'
        WHEN 'P06' THEN 'P04'
        WHEN 'P07' THEN 'P08'
        WHEN 'P08' THEN 'P10'
        WHEN 'P09' THEN 'P03'
        ELSE 'P02'
    END AS next_recommended_pack_id,
    CASE pack_id
        WHEN 'P01' THEN 'Evoluir do Lakehouse para Agentes IA com MCP Server sobre dados Gold'
        WHEN 'P02' THEN 'Adicionar Auditoria de Vies, Seguranca e Governanca de IA em producao'
        WHEN 'P03' THEN 'Consolidar dados geoespaciais em Lakehouse analitico corporativo'
        WHEN 'P04' THEN 'Expandir onboarding KYB para monitoramento continuo de fornecedores (TPRM)'
        WHEN 'P05' THEN 'Integrar series macroeconomicas ao Data Lakehouse corporativo'
        WHEN 'P06' THEN 'Automatizar onboarding KYB dos parceiros participantes da Clean Room'
        WHEN 'P07' THEN 'Blindar pipelines de TPRM com observabilidade e auditoria de qualidade'
        WHEN 'P08' THEN 'Contratar Retainer SRE para observabilidade continua e SLA 24/7'
        WHEN 'P09' THEN 'Acoplar aplicacao FullStack & geolocalizacao 5G para equipe de campo'
        ELSE 'Habilitar camada de IA Agentica sobre dados monitorados com alta confiabilidade'
    END AS tese_expansao_arr,
    ROUND(score_oportunidade_pack_uf * 0.85, 2) AS score_propensao_upsell,
    recomendacao_operacional_pack,
    reference_period
FROM base
ORDER BY pack_id ASC, score_propensao_upsell DESC, sigla_uf ASC
