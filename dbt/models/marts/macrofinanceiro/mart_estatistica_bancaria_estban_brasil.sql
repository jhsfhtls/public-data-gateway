{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        data_base_yyyymm,
        qtd_instituicoes_atuantes,
        qtd_agencias_ativas,
        ativo_total_agencias_brl,
        operacoes_credito_total_brl,
        depositos_totais_brl,
        relacao_credito_deposito_pct,
        indice_concentracao_hhi_local,
        emprestimos_titulos_descontados_brl,
        financiamentos_gerais_brl,
        financiamento_agropecuario_rural_brl,
        financiamento_imobiliario_habitacional_brl,
        provisao_credito_liquidacao_duvidosa_brl,
        taxa_provisao_inadimplencia_pct,
        depositos_vista_brl,
        depositos_poupanca_brl,
        depositos_prazo_cdb_rdb_brl,
        depositos_governo_publico_brl,
        liquidez_imediata_caixa_brl,
        qtd_bancos_lideres_amostra,
        top2_concentracao_credito_pct,
        round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.55 * least(100.0, (ln(1.0 + operacoes_credito_total_brl) / 28.5) * 100.0)
                    + 0.25 * least(100.0, (ln(1.0 + depositos_totais_brl) / 28.5) * 100.0)
                    + 0.20 * least(100.0, (1.0 - least(1.0, indice_concentracao_hhi_local / 10000.0)) * 100.0)
                )
            ),
            2
        ) as score_profundidade_bancaria_estban
    from {{ ref('fct_estatistica_bancaria_estban_municipal') }}
)

select
    *,
    case
        when score_profundidade_bancaria_estban >= 75.0
            then 'POLO_FINANCEIRO_ALTA_LIQUIDEZ'
        when score_profundidade_bancaria_estban >= 55.0
            then 'MERCADO_BANCARIO_CONSOLIDADO'
        else 'MERCADO_BANCARIO_EM_EXPANSAO'
    end as classificacao_mercado_bancario_local
from scored
