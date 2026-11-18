{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        total_obras_cno,
        obras_ativas_em_andamento,
        obras_paralisadas_suspensas,
        obras_encerradas_habitese,
        area_total_registrada_m2,
        area_residencial_m2,
        area_comercial_industrial_m2,
        area_infraestrutura_publica_m2,
        area_total_construida_m2,
        percentual_obras_ativas_pct,
        qtd_obras_amostra_cadastral,
        area_media_obra_amostra_m2,
        qtd_responsaveis_amostra,
        round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.45 * least(100.0, (ln(1.0 + total_obras_cno) / 11.2) * 100.0)
                    + 0.35 * least(100.0, (ln(1.0 + area_total_registrada_m2) / 18.0) * 100.0)
                    + 0.20 * least(100.0, percentual_obras_ativas_pct)
                )
            ),
            2
        ) as score_dinamismo_construcao_cno
    from {{ ref('fct_obras_construcao_civil_cno_municipal') }}
)

select
    *,
    case
        when score_dinamismo_construcao_cno >= 75.0
            then 'POLO_EXPANSAO_CONSTRUTIVA_ALTA'
        when score_dinamismo_construcao_cno >= 50.0
            then 'DINAMISMO_CONSTRUTIVO_MODERADO'
        else 'BAIXA_ATIVIDADE_CNO'
    end as classificacao_expansao_imobiliaria_cno
from scored
