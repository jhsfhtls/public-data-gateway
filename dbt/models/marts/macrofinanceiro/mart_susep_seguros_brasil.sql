{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        premios_emitidos_total_mil_brl,
        sinistros_ocorridos_mil_brl,
        taxa_sinistralidade_media_pct,
        corretoras_ativas_susep,
        premio_per_capita_brl,
        qtd_ramos_seguro_susep,
        premios_amostra_ramos_mil_brl,
        contribuicoes_previdencia_mil_brl,
        resgates_pagamentos_mil_brl,
        provisoes_tecnicas_mi_brl,
        participacao_vgbl_pct,
        agentes_corretor_pj,
        indice_solvencia_regulatoria_pct,
        indice_cobertura_provisoes_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.40 * least(100.0, (ln(1.0 + premios_emitidos_total_mil_brl) / 19.0) * 100.0)
                    + 0.35 * least(100.0, (indice_solvencia_regulatoria_pct / 170.0) * 100.0)
                    + 0.25 * greatest(0.0, 100.0 - taxa_sinistralidade_media_pct * 0.9)
                )
            ),
            2
        ) as score_maturidade_seguros_susep
    from {{ ref('fct_susep_seguros_municipal') }}
)

select
    *,
    case
        when score_maturidade_seguros_susep >= 78.0
            then 'HUB_SEGUROS_SOLVENCIA_ALTA'
        when score_maturidade_seguros_susep >= 58.0
            then 'MERCADO_SEGURADOR_CONSOLIDADO'
        else 'PENETRACAO_SEGURADORA_EXPANSAO'
    end as tier_mercado_segurador_susep
from scored
