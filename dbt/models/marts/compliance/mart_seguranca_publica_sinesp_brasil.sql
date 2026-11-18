{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        taxa_mvi_por_100k_hab,
        taxa_roubo_carga_100k_hab,
        taxa_furto_roubo_veiculo_100k_hab,
        taxa_estelionato_100k_hab,
        investimento_seguranca_per_capita_brl,
        total_vitimas_mvi,
        taxa_elucidacao_inqueritos_pct,
        qtd_delegacias_unidades,
        possui_guarda_municipal_ativa,
        efetivo_seguranca_100k_hab,
        qtd_tipos_crime_amostra,
        qtd_ocorrencias_amostra_sinesp,
        round(
            least(
                98.0,
                greatest(
                    20.0,
                    0.40 * greatest(0.0, 100.0 - taxa_mvi_por_100k_hab * 1.9)
                    + 0.30 * greatest(0.0, 100.0 - taxa_roubo_carga_100k_hab * 1.6)
                    + 0.30 * least(100.0, taxa_elucidacao_inqueritos_pct * 1.35)
                )
            ),
            2
        ) as score_seguranca_publica_sinesp
    from {{ ref('fct_seguranca_publica_sinesp_municipal') }}
)

select
    *,
    case
        when score_seguranca_publica_sinesp >= 75.0
            then 'ALTA_SEGURANCA_TERRITORIAL'
        when score_seguranca_publica_sinesp >= 50.0
            then 'RISCO_SEGURANCA_MODERADO'
        else 'ZONA_CRITICA_SEGURANCA_PATRIMONIAL'
    end as tier_risco_seguranca_territorial
from scored
