{{ config(materialized='table') }}

WITH base AS (
    SELECT
        i.pack_id,
        i.pack_name,
        i.sigla_uf,
        i.regiao,
        i.total_estabelecimentos_ativos AS tam_empresas_ativas_uf,
        i.valor_homologado_compras_uf AS sam_compras_publicas_uf_brl,
        COALESCE(m.taxa_economia_licitatoria_uf_pct, 5.0) AS economia_media_licitatoria_pct,
        i.score_oportunidade_pack_uf,
        i.reference_period
    FROM {{ ref('mart_solution_packs_insights_brasil') }} i
    LEFT JOIN {{ ref('mart_cross_domain_territorio_b2b_macro_brasil') }} m
        ON i.sigla_uf = m.sigla_uf
)

SELECT
    pack_id,
    pack_name,
    sigla_uf,
    regiao,
    tam_empresas_ativas_uf,
    sam_compras_publicas_uf_brl,
    economia_media_licitatoria_pct,
    ROUND(
        CASE
            WHEN pack_id IN ('P01', 'P02') THEN 22.0 + (score_oportunidade_pack_uf * 0.15)
            WHEN pack_id IN ('P04', 'P07') THEN 18.0 + (score_oportunidade_pack_uf * 0.12)
            WHEN pack_id = 'P05' THEN 15.0 + economia_media_licitatoria_pct
            ELSE 16.0 + (score_oportunidade_pack_uf * 0.10)
        END,
        2
    ) AS ancora_roi_estimado_pct,
    CASE
        WHEN score_oportunidade_pack_uf >= 75.0 THEN '2 a 4 meses'
        WHEN score_oportunidade_pack_uf >= 55.0 THEN '4 a 6 meses'
        ELSE '6 a 9 meses'
    END AS faixa_payback_meses,
    reference_period
FROM base
ORDER BY pack_id ASC, ancora_roi_estimado_pct DESC, sigla_uf ASC
