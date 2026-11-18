{{ config(materialized='table') }}

WITH macro_latest AS (
    SELECT
        ano_mes,
        ipca_acumulado_12m_pct,
        selic_meta_fim_mes_pct,
        juros_real_ex_post_pct,
        regime_politica_monetaria
    FROM {{ ref('mart_cenario_macroeconomico_brasil') }}
    ORDER BY ano_mes DESC
    LIMIT 1
),

territorio_uf AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        COUNT(DISTINCT id_municipio) AS qtd_municipios_uf,
        ROUND(SUM(pib_mil_reais), 2) AS pib_total_uf_mil_reais,
        SUM(populacao_residente) AS populacao_total_uf,
        ROUND(AVG(pib_per_capita_reais), 2) AS pib_per_capita_medio_uf,
        ROUND(AVG(indice_gini_uf), 4) AS indice_gini_uf
    FROM {{ ref('mart_perfil_socioeconomico_municipios') }}
    GROUP BY sigla_uf
),

emprego_uf AS (
    SELECT
        sigla_uf,
        SUM(saldo_movimentacao) AS saldo_emprego_formal_uf,
        ROUND(AVG(salario_medio_admissao_reais), 2) AS salario_medio_admissao_uf,
        ROUND(AVG(score_aquecimento_mercado_trabalho), 2) AS score_aquecimento_trabalho_uf
    FROM {{ ref('mart_dinamica_emprego_formal_brasil') }}
    GROUP BY sigla_uf
),

b2b_uf AS (
    SELECT
        sigla_uf,
        total_estabelecimentos_ativos,
        total_empresas_distintas,
        ROUND(capital_social_total_uf, 2) AS capital_social_total_uf,
        taxa_optantes_simples_pct
    FROM {{ ref('mart_densidade_empresarial_brasil') }}
),

compras_uf AS (
    SELECT
        sigla_uf,
        SUM(total_contratacoes) AS total_contratacoes_publicas_uf,
        ROUND(SUM(valor_total_homologado), 2) AS valor_homologado_compras_uf,
        ROUND(AVG(taxa_economia_licitatoria_pct), 2) AS taxa_economia_licitatoria_uf_pct
    FROM {{ ref('mart_compras_publicas_brasil') }}
    GROUP BY sigla_uf
),

compliance_uf AS (
    SELECT
        sigla_uf_orgao AS sigla_uf,
        SUM(total_sancoes_ativas) AS total_sancoes_ativas_uf,
        ROUND(SUM(valor_multas_ativas), 2) AS valor_multas_ativas_uf
    FROM {{ ref('mart_compliance_sancoes_brasil') }}
    GROUP BY sigla_uf_orgao
),

siconfi_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(despesa_pessoal_rcl_pct), 2) AS avg_despesa_pessoal_rcl_siconfi_pct,
        ROUND(AVG(score_solidez_contabil_siconfi), 2) AS avg_score_solidez_siconfi_uf
    FROM {{ ref('mart_contabilidade_fiscal_siconfi') }}
    GROUP BY sigla_uf
),

capag_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(ifgf_geral), 4) AS avg_ifgf_gestao_fiscal_uf,
        ROUND(AVG(CASE WHEN elegivel_garantia_uniao THEN 100.0 ELSE 0.0 END), 2) AS pct_municipios_elegiveis_garantia_capag_uf
    FROM {{ ref('mart_capacidade_pagamento_capag_ifgf_brasil') }}
    GROUP BY sigla_uf
),

estban_uf AS (
    SELECT
        sigla_uf,
        ROUND(SUM(operacoes_credito_total_brl), 2) AS volume_credito_bancario_estban_uf_brl,
        ROUND(SUM(depositos_totais_brl), 2) AS volume_depositos_estban_uf_brl
    FROM {{ ref('mart_estatistica_bancaria_estban_brasil') }}
    GROUP BY sigla_uf
),

agencias_uf AS (
    SELECT
        sigla_uf,
        SUM(total_agencias_fisicas) AS total_agencias_bancarias_bcb_uf,
        ROUND(AVG(score_capilaridade_bancaria_bcb), 2) AS avg_score_capilaridade_bancaria_uf
    FROM {{ ref('mart_capilaridade_agencias_bancarias_brasil') }}
    GROUP BY sigla_uf
),

base AS (
    SELECT
        t.sigla_uf,
        t.regiao,
        t.qtd_municipios_uf,
        t.pib_total_uf_mil_reais,
        t.populacao_total_uf,
        t.pib_per_capita_medio_uf,
        t.indice_gini_uf,
        COALESCE(e.saldo_emprego_formal_uf, 0) AS saldo_emprego_formal_uf,
        COALESCE(e.salario_medio_admissao_uf, 2400.0) AS salario_medio_admissao_uf,
        COALESCE(e.score_aquecimento_trabalho_uf, 50.0) AS score_aquecimento_trabalho_uf,
        COALESCE(b.total_estabelecimentos_ativos, 0) AS total_estabelecimentos_ativos,
        COALESCE(b.total_empresas_distintas, 0) AS total_empresas_distintas,
        COALESCE(b.capital_social_total_uf, 0.0) AS capital_social_total_uf,
        COALESCE(b.taxa_optantes_simples_pct, 0.0) AS taxa_optantes_simples_pct,
        COALESCE(c.total_contratacoes_publicas_uf, 0) AS total_contratacoes_publicas_uf,
        COALESCE(c.valor_homologado_compras_uf, 0.0) AS valor_homologado_compras_uf,
        COALESCE(c.taxa_economia_licitatoria_uf_pct, 0.0) AS taxa_economia_licitatoria_uf_pct,
        COALESCE(s.total_sancoes_ativas_uf, 0) AS total_sancoes_ativas_uf,
        COALESCE(s.valor_multas_ativas_uf, 0.0) AS valor_multas_ativas_uf,
        COALESCE(sf.avg_despesa_pessoal_rcl_siconfi_pct, 45.0) AS avg_despesa_pessoal_rcl_siconfi_pct,
        COALESCE(sf.avg_score_solidez_siconfi_uf, 65.0) AS avg_score_solidez_siconfi_uf,
        COALESCE(cg.avg_ifgf_gestao_fiscal_uf, 0.0) AS avg_ifgf_gestao_fiscal_uf,
        COALESCE(cg.pct_municipios_elegiveis_garantia_capag_uf, 0.0) AS pct_municipios_elegiveis_garantia_capag_uf,
        COALESCE(eb.volume_credito_bancario_estban_uf_brl, 0.0) AS volume_credito_bancario_estban_uf_brl,
        COALESCE(eb.volume_depositos_estban_uf_brl, 0.0) AS volume_depositos_estban_uf_brl,
        COALESCE(ag.total_agencias_bancarias_bcb_uf, 0) AS total_agencias_bancarias_bcb_uf,
        COALESCE(ag.avg_score_capilaridade_bancaria_uf, 0.0) AS avg_score_capilaridade_bancaria_uf,
        COALESCE(m.ano_mes, '2025-03') AS macro_ano_mes_referencia,
        COALESCE(m.selic_meta_fim_mes_pct, 14.25) AS selic_meta_fim_mes_pct,
        COALESCE(m.ipca_acumulado_12m_pct, 5.06) AS ipca_acumulado_12m_pct,
        COALESCE(m.juros_real_ex_post_pct, 8.75) AS juros_real_ex_post_pct,
        COALESCE(m.regime_politica_monetaria, 'CONTRACIONISTA') AS regime_politica_monetaria
    FROM territorio_uf t
    LEFT JOIN emprego_uf e ON t.sigla_uf = e.sigla_uf
    LEFT JOIN b2b_uf b ON t.sigla_uf = b.sigla_uf
    LEFT JOIN compras_uf c ON t.sigla_uf = c.sigla_uf
    LEFT JOIN compliance_uf s ON t.sigla_uf = s.sigla_uf
    LEFT JOIN siconfi_uf sf ON t.sigla_uf = sf.sigla_uf
    LEFT JOIN capag_uf cg ON t.sigla_uf = cg.sigla_uf
    LEFT JOIN estban_uf eb ON t.sigla_uf = eb.sigla_uf
    LEFT JOIN agencias_uf ag ON t.sigla_uf = ag.sigla_uf
    CROSS JOIN macro_latest m
),

scored AS (
    SELECT
        *,
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    0.35 * LEAST(100.0, (pib_per_capita_medio_uf / 65000.0) * 100.0)
                    + 0.25 * score_aquecimento_trabalho_uf
                    + 0.25 * LEAST(100.0, (LN(1.0 + total_estabelecimentos_ativos) / 14.0) * 100.0)
                    + 0.15 * LEAST(100.0, (LN(1.0 + valor_homologado_compras_uf) / 22.0) * 100.0)
                )
            ),
            2
        ) AS score_dinamismo_b2b_territorial
    FROM base
)

SELECT
    *,
    CASE
        WHEN score_dinamismo_b2b_territorial >= 72.0 THEN 'UF_EPICENTRO_B2B_EXPANSAO'
        WHEN score_dinamismo_b2b_territorial >= 58.0 THEN 'UF_ALTO_POTENCIAL_COMERCIAL'
        WHEN score_dinamismo_b2b_territorial >= 44.0 THEN 'UF_CONSOLIDACAO_REGIONAL'
        else 'UF_FRONTEIRA_DESENVOLVIMENTO'
    END AS tier_oportunidade_b2b_uf
FROM scored
ORDER BY score_dinamismo_b2b_territorial DESC, sigla_uf ASC
