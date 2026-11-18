{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        ano_referencia,
        nota_capag_oficial,
        indicador_1_endividamento_dcl_rcl_pct,
        nota_ind_1,
        indicador_2_poupanca_corrente_pct,
        nota_ind_2,
        indicador_3_liquidez_relativa_pct,
        nota_ind_3,
        elegivel_garantia_uniao,
        qualidade_informacao_ranking_icf,
        nota_capag_uf,
        elegivel_garantia_uniao_uf,
        espaco_fiscal_estimado_brl,
        ifgf_geral,
        ifgf_autonomia,
        ifgf_gastos_pessoal,
        ifgf_liquidez,
        ifgf_investimentos,
        conceito_ifgf,
        estoque_garantias_uniao_brl,
        operacoes_contratadas_com_garantia,
        custo_medio_ponderado_aa_pct,
        honras_aval_acumuladas_brl,
        status_adimplencia_garantias,
        round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.55 * case nota_capag_oficial
                        when 'A+' then 100.0
                        when 'A' then 95.0
                        when 'B' then 72.0
                        when 'C' then 42.0
                        else 20.0
                    end
                    + 0.45 * (ifgf_geral * 100.0)
                )
            ),
            2
        ) as score_risco_soberano_subnacional
    from {{ ref('fct_capacidade_pagamento_capag_municipal') }}
)

select
    *,
    case
        when elegivel_garantia_uniao and score_risco_soberano_subnacional >= 75.0
            then 'GRAU_INVESTIMENTO_GARANTIA_UNIAO'
        when score_risco_soberano_subnacional >= 50.0
            then 'CAPACIDADE_MODERADA_OBSERVACAO'
        else 'ALTO_RISCO_RESTRICAO_GARANTIA'
    end as classificacao_rating_subnacional
from scored
