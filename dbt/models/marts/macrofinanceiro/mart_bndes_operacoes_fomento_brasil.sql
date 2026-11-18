{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        total_operacoes_bndes,
        valor_total_contratado_mil_brl,
        desembolso_anual_mil_brl,
        fomento_verde_esg_pct,
        fomento_per_capita_brl,
        qtd_modalidades_bndes,
        prazo_maximo_amortizacao_meses,
        mpme_participacao_pct,
        ano_referencia,
        juros_medio_tjlp_tlp_aa_pct,
        inadimplencia_carteira_bndes_pct,
        cobertura_fgi_garantias_pct,
        projetos_sustentaveis_count,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.40 * least(100.0, (ln(1.0 + valor_total_contratado_mil_brl) / 17.0) * 100.0)
                    + 0.30 * fomento_verde_esg_pct
                    + 0.30 * cobertura_fgi_garantias_pct
                )
            ),
            2
        ) as score_dinamismo_fomento_bndes
    from {{ ref('fct_bndes_operacoes_fomento_municipal') }}
)

select
    *,
    case
        when score_dinamismo_fomento_bndes >= 75.0
            then 'POLO_ALTO_FOMENTO_BNDES'
        when score_dinamismo_fomento_bndes >= 55.0
            then 'FOMENTO_PRODUTIVO_CONSOLIDADO'
        else 'BAIXA_PENETRACAO_FOMENTO'
    end as tier_maturidade_fomento_bndes
from scored
