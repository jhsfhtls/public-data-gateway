{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        rodovia_br_principal,
        total_sinistros_prf_ano,
        total_feridos_prf_ano,
        total_obitos_prf_ano,
        taxa_letalidade_sinistro_pct,
        qtd_agrupamentos_sinistro_prf,
        extensao_monitorada_km,
        sinistros_carga_pesada_ano,
        indice_severidade_corredor,
        pct_colisao_frontal_traseira,
        pct_envolvimento_caminhao_onibus,
        pct_condicao_clima_adverso,
        alcoolemia_autuacoes_ano,
        round(
            least(
                98.0,
                greatest(
                    20.0,
                    100.0
                    - 3.5 * taxa_letalidade_sinistro_pct
                    - 3.0 * indice_severidade_corredor
                )
            ),
            2
        ) as score_seguranca_viaria_prf
    from {{ ref('fct_sinistros_rodoviarios_prf_municipal') }}
)

select
    *,
    case
        when score_seguranca_viaria_prf >= 75.0
            then 'SEGURANCA_VIARIA_ALTA'
        when score_seguranca_viaria_prf >= 50.0
            then 'ATENCAO_CORREDOR_RODOVIARIO'
        else 'TRECHO_CRITICO_SINISTRALIDADE'
    end as tier_risco_rodoviario_prf
from scored
