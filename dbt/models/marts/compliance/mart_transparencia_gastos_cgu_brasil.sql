{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        total_gastos_federais_mil_brl,
        total_transferencias_mil_brl,
        taxa_execucao_orcamentaria_pct,
        gasto_federal_per_capita_brl,
        qtd_modalidades_despesa_cgu,
        total_empenhado_cgu_mil_brl,
        repasse_fpm_fundeb_mil_brl,
        repasse_sus_assistencia_mil_brl,
        dependencia_repasses_federais_pct,
        qtd_convenios_ativos,
        emendas_parlamentares_pagas_mil_brl,
        adimplencia_prestacao_contas_pct,
        uso_cpgf_cartao_mil_brl,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.40 * taxa_execucao_orcamentaria_pct
                    + 0.40 * adimplencia_prestacao_contas_pct
                    + 0.20 * greatest(0.0, 100.0 - dependencia_repasses_federais_pct * 2.0)
                )
            ),
            2
        ) as score_integridade_execucao_cgu
    from {{ ref('fct_gastos_publicos_cgu_municipal') }}
)

select
    *,
    case
        when score_integridade_execucao_cgu >= 75.0
            then 'EXCELENCIA_TRANSPARENCIA_EXECUCAO'
        when score_integridade_execucao_cgu >= 50.0
            then 'EXECUCAO_REGULAR_MONITORADA'
        else 'ALERTA_CONCENTRACAO_CONVENIOS'
    end as tier_transparencia_gastos_cgu
from scored
