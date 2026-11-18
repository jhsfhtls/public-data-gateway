{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        eleitorado_apto_total,
        total_candidaturas_aptas,
        comparecimento_pct,
        receita_total_campanha_mil_brl,
        custo_por_eleitor_brl,
        qtd_cargos_apurados_tse,
        max_partidos_competitivos,
        competicao_eleitoral_media_pct,
        abstencao_pct,
        votos_validos_pct,
        ano_eleicao,
        despesa_contratada_mil_brl,
        contas_aprovadas_pct,
        doadores_cpf_cnpj_distintos,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * contas_aprovadas_pct
                    + 0.35 * comparecimento_pct
                    + 0.20 * competicao_eleitoral_media_pct
                )
            ),
            2
        ) as score_integridade_eleitoral_tse
    from {{ ref('fct_eleicoes_tse_municipal') }}
)

select
    *,
    case
        when score_integridade_eleitoral_tse >= 78.0
            then 'ALTA_MATURIDADE_ELEITORAL'
        when score_integridade_eleitoral_tse >= 60.0
            then 'PARTICIPACAO_ELEITORAL_REGULAR'
        else 'ALERTA_ABSTENCAO_CONCENTRACAO'
    end as tier_maturidade_eleitoral_tse
from scored
