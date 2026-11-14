{{ config(materialized='table') }}

WITH base_mun AS (
    SELECT
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        regiao,
        is_capital,
        is_amazonia_legal,
        COALESCE(TRY_CAST(reference_date AS DATE), DATE '2025-03-31') AS as_of_date,
        reference_date
    FROM {{ ref('mart_hierarquia_territorial_brasil') }}
),

socio AS (
    SELECT
        id_municipio,
        pib_mil_reais,
        populacao_residente,
        pib_per_capita_reais
    FROM {{ ref('mart_perfil_socioeconomico_municipios') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

emprego AS (
    SELECT
        id_municipio,
        score_aquecimento_mercado_trabalho
    FROM {{ ref('mart_dinamica_emprego_formal_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY competencia_movimentacao DESC NULLS LAST) = 1
),

saude AS (
    SELECT
        id_municipio,
        score_capacidade_hospitalar
    FROM {{ ref('mart_capacidade_assistencial_saude_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

educacao AS (
    SELECT
        id_municipio,
        score_desempenho_ideb_municipal
    FROM {{ ref('mart_qualidade_educacao_basica_ideb_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

telecom AS (
    SELECT
        id_municipio,
        score_conectividade_digital
    FROM {{ ref('mart_conectividade_digital_telecom_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

esg AS (
    SELECT
        id_municipio,
        score_sustentabilidade_agro_infra
    FROM {{ ref('mart_cross_domain_esg_agro_infra_brasil') }}
),

datalake AS (
    SELECT
        id_municipio,
        score_maturidade_bd
    FROM {{ ref('mart_maturidade_datalake_federado_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

fiscal AS (
    SELECT
        id_municipio_ibge_7d AS id_municipio,
        nota_capag_final,
        razao_dcl_rcl_pct
    FROM {{ ref('mart_saude_fiscal_municipios_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio_ibge_7d ORDER BY ano_exercicio DESC NULLS LAST) = 1
),

uf_macro_b2b AS (
    SELECT
        sigla_uf,
        score_dinamismo_b2b_territorial
    FROM {{ ref('mart_cross_domain_territorio_b2b_macro_brasil') }}
),

macro_series AS (
    SELECT
        COALESCE(TRY_CAST(data_competencia AS DATE), DATE '2025-01-01') AS macro_date,
        selic_meta_fim_mes_pct,
        ipca_acumulado_12m_pct,
        juros_real_ex_post_pct
    FROM {{ ref('mart_cenario_macroeconomico_brasil') }}
),

macro_fallback AS (
    SELECT
        selic_meta_fim_mes_pct,
        ipca_acumulado_12m_pct,
        juros_real_ex_post_pct
    FROM macro_series
    ORDER BY macro_date DESC
    LIMIT 1
),

asof_joined AS (
    SELECT
        m.id_municipio,
        m.nome_municipio,
        m.sigla_uf,
        m.id_uf,
        m.regiao,
        m.is_capital,
        m.is_amazonia_legal,
        m.reference_date,
        COALESCE(s.pib_per_capita_reais, 28000.0) AS pib_per_capita_reais,
        COALESCE(s.populacao_residente, 25000) AS populacao_residente,
        ROUND(LEAST(100.0, GREATEST(0.0, (COALESCE(s.pib_per_capita_reais, 28000.0) / 80000.0) * 100.0)), 2) AS feat_score_socioeconomico,
        ROUND(COALESCE(e.score_aquecimento_mercado_trabalho, 52.0), 2) AS feat_score_dinamica_emprego,
        ROUND(COALESCE(sa.score_capacidade_hospitalar, 50.0), 2) AS feat_score_saude_assistencial,
        ROUND(COALESCE(ed.score_desempenho_ideb_municipal, 55.0), 2) AS feat_score_educacao_ideb,
        ROUND(COALESCE(t.score_conectividade_digital, 50.0), 2) AS feat_score_conectividade_5g,
        ROUND(COALESCE(es.score_sustentabilidade_agro_infra, 60.0), 2) AS feat_score_esg_agro_resiliencia,
        ROUND(COALESCE(dl.score_maturidade_bd, 55.0), 2) AS feat_score_governanca_datalake,
        ROUND(COALESCE(uf.score_dinamismo_b2b_territorial, 55.0), 2) AS feat_score_dinamismo_b2b_uf,
        COALESCE(f.nota_capag_final, 'B') AS nota_capag_final,
        ROUND(COALESCE(f.razao_dcl_rcl_pct, 45.0), 2) AS razao_dcl_rcl_pct,
        ROUND(COALESCE(mac.selic_meta_fim_mes_pct, mf.selic_meta_fim_mes_pct, 14.25), 2) AS macro_selic_asof_pct,
        ROUND(COALESCE(mac.ipca_acumulado_12m_pct, mf.ipca_acumulado_12m_pct, 5.06), 2) AS macro_ipca_12m_asof_pct,
        ROUND(COALESCE(mac.juros_real_ex_post_pct, mf.juros_real_ex_post_pct, 8.75), 2) AS macro_juros_real_asof_pct
    FROM base_mun m
    ASOF LEFT JOIN macro_series mac ON m.as_of_date >= mac.macro_date
    LEFT JOIN socio s ON m.id_municipio = s.id_municipio
    LEFT JOIN emprego e ON m.id_municipio = e.id_municipio
    LEFT JOIN saude sa ON m.id_municipio = sa.id_municipio
    LEFT JOIN educacao ed ON m.id_municipio = ed.id_municipio
    LEFT JOIN telecom t ON m.id_municipio = t.id_municipio
    LEFT JOIN esg es ON m.id_municipio = es.id_municipio
    LEFT JOIN datalake dl ON m.id_municipio = dl.id_municipio
    LEFT JOIN fiscal f ON m.id_municipio = f.id_municipio
    LEFT JOIN uf_macro_b2b uf ON m.sigla_uf = uf.sigla_uf
    CROSS JOIN macro_fallback mf
),

final_features AS (
    SELECT
        *,
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    0.22 * feat_score_socioeconomico
                    + 0.18 * feat_score_dinamica_emprego
                    + 0.15 * feat_score_conectividade_5g
                    + 0.15 * feat_score_dinamismo_b2b_uf
                    + 0.15 * feat_score_governanca_datalake
                    + 0.15 * feat_score_educacao_ideb
                )
            ),
            2
        ) AS feat_score_propensao_b2b_composto,
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    0.40 * (100.0 - feat_score_esg_agro_resiliencia)
                    + 0.35 * LEAST(100.0, GREATEST(0.0, razao_dcl_rcl_pct))
                    + 0.25 * (CASE WHEN nota_capag_final = 'C' THEN 85.0 WHEN nota_capag_final = 'B' THEN 45.0 ELSE 15.0 END)
                )
            ),
            2
        ) AS feat_score_risco_compliance_fiscal
    FROM asof_joined
)

SELECT
    *,
    CASE
        WHEN feat_score_propensao_b2b_composto >= 72.0 AND feat_score_risco_compliance_fiscal <= 40.0
            THEN 'PLATINA_CROSS_DOMAIN'
        WHEN feat_score_propensao_b2b_composto >= 60.0
            THEN 'OURO_ALTA_PROPENSAO'
        WHEN feat_score_propensao_b2b_composto >= 48.0
            THEN 'PRATA_EXPANSAO_REGIONAL'
        ELSE 'BRONZE_FRONTEIRA_DADOS'
    END AS tier_maturidade_cross_domain
FROM final_features
ORDER BY feat_score_propensao_b2b_composto DESC, id_municipio ASC
