{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        bioma_predominante,
        qtd_imoveis_cafir,
        area_total_cafir_ha,
        area_media_imovel_cafir_ha,
        vtn_medio_estimado_brl_ha,
        taxa_vinculacao_cnir_pct,
        taxa_georreferenciamento_sigef_pct,
        paridade_cib_car_pct,
        imoveis_pendentes_vinculacao_cnt,
        indice_gini_fundiario_cafir,
        area_pequenos_imoveis_ate_4mf_pct,
        area_medios_imoveis_4a15mf_pct,
        area_grandes_imoveis_acima_15mf_pct,
        qtd_imoveis_amostra_cafir,
        area_media_amostra_ha,
        round(
            least(
                98.0,
                greatest(
                    15.0,
                    0.40 * least(100.0, taxa_vinculacao_cnir_pct)
                    + 0.35 * least(100.0, taxa_georreferenciamento_sigef_pct)
                    + 0.25 * least(100.0, paridade_cib_car_pct)
                )
            ),
            2
        ) as score_regularizacao_fundiaria_cafir
    from {{ ref('fct_imoveis_rurais_cafir_municipal') }}
)

select
    *,
    case
        when score_regularizacao_fundiaria_cafir >= 75.0
            then 'ESTRUTURA_CNIR_REGULARIZADA'
        when score_regularizacao_fundiaria_cafir >= 50.0
            then 'CONSOLIDACAO_CADASTRAL_PARCIAL'
        else 'BAIXA_VINCULACAO_CNIR_CAFIR'
    end as classificacao_estrutura_fundiaria_cafir
from scored
