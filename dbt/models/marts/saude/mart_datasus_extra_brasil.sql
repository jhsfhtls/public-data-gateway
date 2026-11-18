{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        producao_sia_total_12m,
        notificacoes_sinan_total_12m,
        valor_sia_aprovado_mil_brl,
        cobertura_sisvan_pct,
        atendimentos_sia_per_capita,
        qtd_linhas_procedimento_sia,
        valor_amostra_sia_mil_brl,
        notificacoes_dengue_12m,
        notificacoes_respiratorias_12m,
        taxa_incidencia_100k_hab,
        oportunidade_encerramento_pct,
        prevalencia_obesidade_adulto_pns_pct,
        acompanhamento_condicoes_cronicas_pct,
        inseguranca_alimentar_grave_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * oportunidade_encerramento_pct
                    + 0.30 * cobertura_sisvan_pct
                    + 0.25 * acompanhamento_condicoes_cronicas_pct
                )
            ),
            2
        ) as score_resolutividade_sus_datasus
    from {{ ref('fct_datasus_extra_municipal') }}
)

select
    *,
    case
        when score_resolutividade_sus_datasus >= 78.0
            then 'POLO_ALTA_RESOLUTIVIDADE_SUS'
        when score_resolutividade_sus_datasus >= 60.0
            then 'REDE_AMBULATORIAL_CONSOLIDADA'
        else 'VULNERABILIDADE_VIGILANCIA_SUS'
    end as tier_capacidade_ambulatorial_sus
from scored
