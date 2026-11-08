{{ config(materialized='table') }}

WITH cotacoes_usd_raw AS (
    SELECT
        data_cotacao,
        ano_mes,
        cotacao_compra AS dolar_compra,
        cotacao_venda AS dolar_venda,
        ROW_NUMBER() OVER (PARTITION BY data_cotacao ORDER BY data_hora_cotacao DESC) AS rn
    FROM {{ ref('stg_bcb_ptax_dolar') }}
),

cotacoes_usd AS (
    SELECT
        data_cotacao,
        ano_mes,
        dolar_compra,
        dolar_venda
    FROM cotacoes_usd_raw
    WHERE rn = 1
),

cotacoes_eur_raw AS (
    SELECT
        data_cotacao,
        cotacao_compra AS euro_compra,
        cotacao_venda AS euro_venda,
        ROW_NUMBER() OVER (PARTITION BY data_cotacao ORDER BY data_hora_cotacao DESC) AS rn
    FROM {{ ref('stg_bcb_ptax_moedas_cotacoes') }}
    WHERE simbolo_moeda = 'EUR' AND tipo_boletim LIKE '%Fechamento%'
),

cotacoes_eur AS (
    SELECT
        data_cotacao,
        euro_compra,
        euro_venda
    FROM cotacoes_eur_raw
    WHERE rn = 1
),

cotacoes_gbp_raw AS (
    SELECT
        data_cotacao,
        cotacao_compra AS libra_compra,
        cotacao_venda AS libra_venda,
        ROW_NUMBER() OVER (PARTITION BY data_cotacao ORDER BY data_hora_cotacao DESC) AS rn
    FROM {{ ref('stg_bcb_ptax_moedas_cotacoes') }}
    WHERE simbolo_moeda = 'GBP' AND tipo_boletim LIKE '%Fechamento%'
),

cotacoes_gbp AS (
    SELECT
        data_cotacao,
        libra_compra,
        libra_venda
    FROM cotacoes_gbp_raw
    WHERE rn = 1
),

all_dates AS (
    SELECT DISTINCT data_cotacao, ano_mes FROM cotacoes_usd
)

SELECT
    d.data_cotacao,
    CAST(d.data_cotacao AS DATE) AS data_referencia,
    d.ano_mes,
    u.dolar_compra,
    u.dolar_venda,
    ROUND(u.dolar_venda - u.dolar_compra, 4) AS dolar_spread,
    e.euro_compra,
    e.euro_venda,
    g.libra_compra,
    g.libra_venda
FROM all_dates d
JOIN cotacoes_usd u ON d.data_cotacao = u.data_cotacao
LEFT JOIN cotacoes_eur e ON d.data_cotacao = e.data_cotacao
LEFT JOIN cotacoes_gbp g ON d.data_cotacao = g.data_cotacao
ORDER BY d.data_cotacao
