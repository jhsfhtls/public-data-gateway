{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        distribuidora_principal,
        qtd_usinas_mmgd,
        potencia_instalada_total_kw,
        potencia_solar_ufv_kw,
        potencia_rural_agro_kw,
        ucs_com_credito_energia,
        participacao_solar_pct,
        prazo_medio_vistoria_dias,
        taxa_aprovacao_parecer_pct,
        inversao_fluxo_critica_pct,
        ucs_beneficiarias_total,
        geracao_estimada_mwh_ano,
        potencia_bioenergia_kw,
        qtd_empreendimentos_amostra_mmgd,
        potencia_amostra_kw,
        round(
            least(
                98.0,
                greatest(
                    20.0,
                    0.45 * least(100.0, (potencia_instalada_total_kw / 480000.0) * 100.0)
                    + 0.30 * least(100.0, (qtd_usinas_mmgd / 50000.0) * 100.0)
                    + 0.25 * greatest(0.0, taxa_aprovacao_parecer_pct - inversao_fluxo_critica_pct * 0.6)
                )
            ),
            2
        ) as score_transicao_energetica_mmgd
    from {{ ref('fct_geracao_distribuida_aneel_municipal') }}
)

select
    *,
    case
        when score_transicao_energetica_mmgd >= 75.0
            then 'POLO_SOLAR_MMGD_AVANCADO'
        when score_transicao_energetica_mmgd >= 50.0
            then 'EXPANSAO_RENOVAVEL_CONSOLIDADA'
        else 'TRANSICAO_MMGD_EMERGENTE'
    end as tier_maturidade_solar_mmgd
from scored
