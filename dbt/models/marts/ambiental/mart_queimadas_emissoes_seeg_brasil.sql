{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        bioma_predominante,
        qtd_focos_calor_inpe,
        area_cicatriz_queimada_ha,
        risco_fogo_medio_inpe,
        emissoes_liquidas_tco2e_seeg,
        participacao_agro_lulucf_pct,
        frp_medio_mw,
        dias_seco_consecutivos_max,
        concentracao_estacao_seca_pct,
        qtd_focos_amostra_inpe,
        frp_medio_amostra_mw,
        emissoes_amostra_tco2e,
        remocao_florestal_tco2e,
        round(
            least(
                98.0,
                greatest(
                    12.0,
                    0.40 * greatest(0.0, 100.0 - (ln(1.0 + qtd_focos_calor_inpe) / 8.0) * 100.0)
                    + 0.30 * greatest(0.0, 100.0 - risco_fogo_medio_inpe * 100.0)
                    + 0.30 * greatest(0.0, 100.0 - (ln(1.0 + emissoes_liquidas_tco2e_seeg / 1000000.0) / 4.2) * 100.0)
                )
            ),
            2
        ) as score_resiliencia_climatica_fogo_gee
    from {{ ref('fct_queimadas_emissoes_gee_municipal') }}
)

select
    *,
    case
        when score_resiliencia_climatica_fogo_gee >= 75.0
            then 'BAIXO_RISCO_FOGO_GEE'
        when score_resiliencia_climatica_fogo_gee >= 50.0
            then 'ALERTA_FOGO_EMISSOES_MODERADAS'
        else 'CRITICO_QUEIMADAS_EMISSOES_GEE'
    end as classificacao_risco_climatico_queimadas
from scored
