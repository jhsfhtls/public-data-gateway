{{ config(materialized='table') }}

WITH fct_macro AS (
    SELECT
        ano_mes,
        data_competencia,
        ipca_mensal_pct,
        ipca_acumulado_12m_pct,
        igpm_mensal_pct,
        selic_meta_fim_mes_pct,
        selic_diaria_media_mensal_pct,
        cdi_diario_medio_mensal_pct,
        ibcbr_indice
    FROM {{ ref('fct_macro_indicadores_mensal') }}
),

calculos_cenario AS (
    SELECT
        ano_mes,
        data_competencia,
        ipca_mensal_pct,
        ipca_acumulado_12m_pct,
        igpm_mensal_pct,
        selic_meta_fim_mes_pct,
        selic_diaria_media_mensal_pct,
        cdi_diario_medio_mensal_pct,
        ibcbr_indice,
        ROUND(COALESCE(selic_meta_fim_mes_pct, selic_diaria_media_mensal_pct) - ipca_acumulado_12m_pct, 2) AS juros_real_ex_post_pct,
        ROUND(cdi_diario_medio_mensal_pct - selic_diaria_media_mensal_pct, 4) AS spread_cdi_selic_pct,
        ROUND(
            ((ibcbr_indice - LAG(ibcbr_indice, 1) OVER (ORDER BY ano_mes)) / NULLIF(LAG(ibcbr_indice, 1) OVER (ORDER BY ano_mes), 0)) * 100,
            2
        ) AS crescimento_ibcbr_mom_pct,
        ROUND(
            ((ibcbr_indice - LAG(ibcbr_indice, 12) OVER (ORDER BY ano_mes)) / NULLIF(LAG(ibcbr_indice, 12) OVER (ORDER BY ano_mes), 0)) * 100,
            2
        ) AS crescimento_ibcbr_yoy_pct
    FROM fct_macro
)

SELECT
    ano_mes,
    data_competencia,
    ipca_mensal_pct,
    ipca_acumulado_12m_pct,
    igpm_mensal_pct,
    selic_meta_fim_mes_pct,
    selic_diaria_media_mensal_pct,
    cdi_diario_medio_mensal_pct,
    ibcbr_indice,
    juros_real_ex_post_pct,
    spread_cdi_selic_pct,
    crescimento_ibcbr_mom_pct,
    crescimento_ibcbr_yoy_pct,
    CASE
        WHEN juros_real_ex_post_pct > 4.5 THEN 'CONTRACIONISTA'
        WHEN juros_real_ex_post_pct BETWEEN 3.0 AND 4.5 THEN 'NEUTRO'
        WHEN juros_real_ex_post_pct < 3.0 THEN 'EXPANSIONISTA'
        ELSE 'INDETERMINADO'
    END AS regime_politica_monetaria,
    CASE
        WHEN ipca_acumulado_12m_pct > 4.5 THEN 'ACIMA_DO_TETO_META'
        WHEN ipca_acumulado_12m_pct BETWEEN 1.5 AND 4.5 THEN 'DENTRO_DO_INTERVALO_TOLERANCIA'
        WHEN ipca_acumulado_12m_pct < 1.5 THEN 'ABAIXO_DO_PISO_META'
        ELSE 'NAO_APLICAVEL'
    END AS classificacao_inflacao_meta
FROM calculos_cenario
ORDER BY ano_mes
