{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        total_servidores_federais_siape,
        folha_mensal_bruta_siape_mil_brl,
        remuneracao_media_bruta_brl,
        pct_cargos_comissionados_das,
        qtd_regimes_lotacao_siape,
        remuneracao_teto_pct,
        adicional_funcao_das_fce_mil_brl,
        servidores_dedicacao_exclusiva_pct,
        alertas_acumulo_ilicito,
        servidores_cedidos_count,
        indice_integridade_siape_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.50 * indice_integridade_siape_pct
                    + 0.35 * servidores_dedicacao_exclusiva_pct
                    + 0.15 * greatest(0.0, 100.0 - pct_cargos_comissionados_das * 2.0)
                )
            ),
            2
        ) as score_integridade_funcional_siape
    from {{ ref('fct_servidores_federais_siape_municipal') }}
)

select
    *,
    case
        when score_integridade_funcional_siape >= 75.0
            then 'ALTA_GOVERNANCA_FUNCIONAL_SIAPE'
        when score_integridade_funcional_siape >= 50.0
            then 'QUADRO_FEDERAL_CONSOLIDADO'
        else 'ALERTA_CONCENTRACAO_COMISSIONADOS'
    end as tier_governanca_pessoal_siape
from scored
