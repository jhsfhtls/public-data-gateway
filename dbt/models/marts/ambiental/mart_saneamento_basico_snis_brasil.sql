{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        prestador_servico,
        natureza_juridica_prestador,
        atendimento_agua_total_pct,
        atendimento_esgoto_total_pct,
        esgoto_tratado_sobre_consumido_pct,
        indice_perdas_distribuicao_pct,
        cobertura_coleta_rdo_pct,
        investimento_total_milhoes_brl,
        coleta_seletiva_ativa,
        destinacao_adequada_aterro_pct,
        massa_recuperada_reciclagem_t,
        tarifa_media_agua_esgoto_m3,
        indice_suficiencia_caixa_pct,
        investimento_anual_per_capita_brl,
        round(
            least(
                98.0,
                greatest(
                    20.0,
                    0.35 * atendimento_agua_total_pct
                    + 0.35 * atendimento_esgoto_total_pct
                    + 0.15 * esgoto_tratado_sobre_consumido_pct
                    + 0.15 * greatest(0.0, 100.0 - indice_perdas_distribuicao_pct)
                )
            ),
            2
        ) as score_universalizacao_saneamento_snis
    from {{ ref('fct_saneamento_basico_snis_municipal') }}
)

select
    *,
    case
        when score_universalizacao_saneamento_snis >= 75.0
            then 'UNIVERSALIZADO_META_2033'
        when score_universalizacao_saneamento_snis >= 50.0
            then 'EM_CONSOLIDACAO_REGULATORIA'
        else 'DEFICIT_SANEAMENTO_CRITICO'
    end as tier_marco_saneamento_2033
from scored
