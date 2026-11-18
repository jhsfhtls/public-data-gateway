{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        frota_total_veiculos,
        qtd_automoveis,
        qtd_motocicletas,
        qtd_caminhoes_utilitarios,
        qtd_eletrificados_bevs_hevs,
        veiculos_novos_emplacados_ano,
        participacao_eletrificados_pct,
        idade_media_frota_anos,
        participacao_frota_0a5_anos_pct,
        taxa_motorizacao_100hab,
        participacao_caminhoes_pct,
        qtd_categorias_frota,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.35 * least(100.0, participacao_frota_0a5_anos_pct * 2.1)
                    + 0.35 * least(100.0, participacao_eletrificados_pct * 50.0)
                    + 0.30 * least(100.0, taxa_motorizacao_100hab * 1.1)
                )
            ),
            2
        ) as score_modernizacao_frota_mobilidade
    from {{ ref('fct_frota_veiculos_senatran_municipal') }}
)

select
    *,
    case
        when score_modernizacao_frota_mobilidade >= 75.0
            then 'POLO_MOBILIDADE_FROTA_MODERNA'
        when score_modernizacao_frota_mobilidade >= 50.0
            then 'FROTA_URBANA_CONSOLIDADA'
        else 'FROTA_TRANSICAO_ENVELHECIDA'
    end as tier_mobilidade_frota_senatran
from scored
