{{ config(materialized='table') }}

with infra as (
    select
        *,
        least(100.0, greatest(0.0, (densidade_erbs_por_10k_hab / 20.0) * 100.0)) as densidade_norm,
        least(100.0, greatest(0.0, (velocidade_mediana_mbps / 400.0) * 100.0)) as velocidade_norm,
        least(100.0, greatest(0.0, (proporcao_erbs_5g_pct / 40.0) * 100.0)) as proporcao_5g_norm
    from {{ ref('fct_anatel_infraestrutura_telecom_municipio') }}
),

espectro_agg as (
    select
        id_municipio,
        sum(largura_banda_agregada_mhz) as largura_banda_total_mhz,
        sum(case when faixa_frequencia_mhz = '3500 MHz' then estacoes_licenciadas_faixa else 0 end) as estacoes_3500mhz_5g_sa
    from {{ ref('fct_anatel_alocacao_espectro_municipio') }}
    group by id_municipio
),

scored as (
    select
        i.*,
        coalesce(e.largura_banda_total_mhz, 0.0) as largura_banda_total_mhz,
        coalesce(e.estacoes_3500mhz_5g_sa, 0) as estacoes_3500mhz_5g_sa,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.55 * i.populacao_coberta_5g_pct
                    + 0.30 * i.proporcao_5g_norm
                    + 0.15 * i.densidade_norm
                )
            ),
            2
        ) as score_maturidade_5g
    from infra i
    left join espectro_agg e on i.id_municipio = e.id_municipio
),

final_scores as (
    select
        *,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.30 * participacao_fibra_ftth_pct
                    + 0.25 * velocidade_norm
                    + 0.25 * score_maturidade_5g
                    + 0.10 * densidade_norm
                    + (case when possui_backhaul_fibra then 10.0 else 0.0 end)
                )
            ),
            2
        ) as score_conectividade_digital
    from scored
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_mes_referencia,
    total_erbs_licenciadas,
    erbs_5g_nr,
    proporcao_erbs_5g_pct,
    densidade_erbs_por_10k_hab,
    densidade_erbs_por_100km2,
    operadoras_ativas_smp,
    operadora_lider_erbs,
    possui_backhaul_fibra,
    total_acessos_scm,
    acessos_fibra_ftth,
    participacao_fibra_ftth_pct,
    velocidade_mediana_mbps,
    prestadoras_scm_ativas,
    indice_hhi_concentracao_scm,
    populacao_coberta_4g_pct,
    populacao_coberta_5g_pct,
    area_geografica_coberta_movel_pct,
    cobertura_rodovias_federais_pct,
    localidades_rurais_conectadas_pct,
    escolas_publicas_conectadas_pct,
    largura_banda_total_mhz,
    estacoes_3500mhz_5g_sa,
    score_maturidade_5g,
    score_conectividade_digital,
    case
        when possui_backhaul_fibra and score_conectividade_digital >= 85.0 and score_maturidade_5g >= 75.0
            then 'HUB_5G_FIBRA_AVANCADO'
        when possui_backhaul_fibra and score_conectividade_digital >= 70.0
            then 'CONECTIVIDADE_CONSOLIDADA'
        when score_conectividade_digital >= 50.0
            then 'EXPANSAO_INFRAESTRUTURA'
        else 'GAP_CONECTIVIDADE'
    end as classificacao_infraestrutura_telecom,
    reference_date,
    extracted_at
from final_scores
