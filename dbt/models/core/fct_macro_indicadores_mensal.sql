{{ config(materialized='table') }}

WITH ipca_mensal AS (
    SELECT
        ano_mes,
        valor AS ipca_mensal_pct
    FROM {{ ref('stg_bcb_sgs_ipca') }}
    WHERE id_serie = '433'
),

ipca_12m AS (
    SELECT
        ano_mes,
        valor AS ipca_acumulado_12m_pct
    FROM {{ ref('stg_bcb_sgs_ipca') }}
    WHERE id_serie = '13522'
),

igpm AS (
    SELECT
        ano_mes,
        valor AS igpm_mensal_pct
    FROM {{ ref('stg_bcb_sgs_igpm') }}
),

selic_meta AS (
    SELECT
        ano_mes,
        valor AS selic_meta_fim_mes_pct,
        ROW_NUMBER() OVER (PARTITION BY ano_mes ORDER BY data_referencia DESC) AS rn
    FROM {{ ref('stg_bcb_sgs_selic') }}
    WHERE id_serie = '432'
),

selic_diaria_agg AS (
    SELECT
        ano_mes,
        ROUND(AVG(valor), 4) AS selic_diaria_media_mensal_pct
    FROM {{ ref('stg_bcb_sgs_selic') }}
    WHERE id_serie = '11'
    GROUP BY ano_mes
),

cdi_diaria_agg AS (
    SELECT
        ano_mes,
        ROUND(AVG(valor), 4) AS cdi_diario_medio_mensal_pct
    FROM {{ ref('stg_bcb_sgs_cdi') }}
    GROUP BY ano_mes
),

ibcbr AS (
    SELECT
        ano_mes,
        valor AS ibcbr_indice
    FROM {{ ref('stg_bcb_sgs_ibcbr') }}
),

all_competencias AS (
    SELECT DISTINCT ano_mes FROM {{ ref('stg_bcb_sgs_ipca') }}
    UNION
    SELECT DISTINCT ano_mes FROM {{ ref('stg_bcb_sgs_igpm') }}
    UNION
    SELECT DISTINCT ano_mes FROM {{ ref('stg_bcb_sgs_selic') }}
    UNION
    SELECT DISTINCT ano_mes FROM {{ ref('stg_bcb_sgs_cdi') }}
    UNION
    SELECT DISTINCT ano_mes FROM {{ ref('stg_bcb_sgs_ibcbr') }}
)

SELECT
    c.ano_mes,
    CAST(c.ano_mes || '-01' AS DATE) AS data_competencia,
    ip.ipca_mensal_pct,
    ip12.ipca_acumulado_12m_pct,
    ig.igpm_mensal_pct,
    sm.selic_meta_fim_mes_pct,
    sd.selic_diaria_media_mensal_pct,
    cd.cdi_diario_medio_mensal_pct,
    ib.ibcbr_indice
FROM all_competencias c
LEFT JOIN ipca_mensal ip ON c.ano_mes = ip.ano_mes
LEFT JOIN ipca_12m ip12 ON c.ano_mes = ip12.ano_mes
LEFT JOIN igpm ig ON c.ano_mes = ig.ano_mes
LEFT JOIN (SELECT ano_mes, selic_meta_fim_mes_pct FROM selic_meta WHERE rn = 1) sm ON c.ano_mes = sm.ano_mes
LEFT JOIN selic_diaria_agg sd ON c.ano_mes = sd.ano_mes
LEFT JOIN cdi_diaria_agg cd ON c.ano_mes = cd.ano_mes
LEFT JOIN ibcbr ib ON c.ano_mes = ib.ano_mes
ORDER BY c.ano_mes
