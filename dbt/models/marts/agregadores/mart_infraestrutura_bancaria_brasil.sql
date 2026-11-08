{{ config(materialized='table') }}

WITH inst AS (
    SELECT
        uf_sede,
        is_banco_comercial,
        participa_pix
    FROM {{ ref('dim_instituicao_financeira') }}
    WHERE uf_sede IS NOT NULL
),

agrupado AS (
    SELECT
        uf_sede,
        COUNT(*) AS total_instituicoes_sede,
        SUM(CASE WHEN is_banco_comercial THEN 1 ELSE 0 END) AS total_bancos_comerciais,
        SUM(CASE WHEN participa_pix THEN 1 ELSE 0 END) AS total_participantes_pix
    FROM inst
    GROUP BY uf_sede
)

SELECT
    uf_sede,
    total_instituicoes_sede,
    total_bancos_comerciais,
    total_participantes_pix,
    ROUND((total_participantes_pix * 100.0) / NULLIF(total_instituicoes_sede, 0), 2) AS pct_adesao_pix,
    CASE
        WHEN total_instituicoes_sede >= 50 THEN 'HUB_FINANCEIRO_NACIONAL'
        WHEN total_instituicoes_sede >= 10 THEN 'POLO_FINANCEIRO_REGIONAL'
        ELSE 'PRESENCA_DISPERSA'
    END AS polo_financeiro_tier
FROM agrupado
ORDER BY total_instituicoes_sede DESC, uf_sede
