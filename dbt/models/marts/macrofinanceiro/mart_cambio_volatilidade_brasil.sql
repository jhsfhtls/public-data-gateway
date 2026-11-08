{{ config(materialized='table') }}

WITH fct_cambio AS (
    SELECT
        data_cotacao,
        data_referencia,
        ano_mes,
        dolar_compra,
        dolar_venda,
        dolar_spread,
        euro_venda
    FROM {{ ref('fct_cambio_ptax_diario') }}
),

calculos_volatilidade AS (
    SELECT
        data_cotacao,
        data_referencia,
        ano_mes,
        dolar_compra,
        dolar_venda,
        dolar_spread,
        euro_venda,
        ROUND(
            ((dolar_venda - LAG(dolar_venda, 1) OVER (ORDER BY data_cotacao)) / NULLIF(LAG(dolar_venda, 1) OVER (ORDER BY data_cotacao), 0)) * 100,
            4
        ) AS variacao_dolar_dia_pct,
        ROUND(
            ((euro_venda - LAG(euro_venda, 1) OVER (ORDER BY data_cotacao)) / NULLIF(LAG(euro_venda, 1) OVER (ORDER BY data_cotacao), 0)) * 100,
            4
        ) AS variacao_euro_dia_pct,
        ROUND(
            AVG(dolar_venda) OVER (ORDER BY data_cotacao ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),
            4
        ) AS media_movel_dolar_21d,
        ROUND(
            STDDEV_SAMP(dolar_venda) OVER (ORDER BY data_cotacao ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
            4
        ) AS volatilidade_dolar_30d
    FROM fct_cambio
)

SELECT
    data_cotacao,
    data_referencia,
    ano_mes,
    dolar_compra,
    dolar_venda,
    dolar_spread,
    euro_venda,
    variacao_dolar_dia_pct,
    variacao_euro_dia_pct,
    media_movel_dolar_21d,
    volatilidade_dolar_30d,
    CASE
        WHEN ABS(COALESCE(variacao_dolar_dia_pct, 0)) > 1.5 THEN 'ALTO_ESTRESSE'
        WHEN ABS(COALESCE(variacao_dolar_dia_pct, 0)) BETWEEN 0.5 AND 1.5 THEN 'VOLATILIDADE_MODERADA'
        ELSE 'ESTABILIDADE'
    END AS regime_estresse_cambial
FROM calculos_volatilidade
ORDER BY data_cotacao
