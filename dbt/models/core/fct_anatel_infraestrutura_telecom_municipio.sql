{{ config(materialized='table') }}

with estacoes as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_mes_referencia,
        total_erbs_licenciadas,
        erbs_5g_nr,
        erbs_4g_lte,
        erbs_3g_wcdma,
        erbs_2g_gsm,
        operadoras_ativas_smp,
        operadora_lider_erbs,
        densidade_erbs_por_10k_hab,
        densidade_erbs_por_100km2,
        cobertura_urbana_5g_pct,
        cobertura_urbana_4g_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_anatel_estacoes_smp') }}
),

banda_larga as (
    select
        id_municipio,
        possui_backhaul_fibra,
        total_acessos_scm,
        acessos_fibra_ftth,
        acessos_cabo_coaxial,
        acessos_radio_satelite,
        participacao_fibra_pct,
        velocidade_mediana_mbps,
        prestadoras_scm_ativas,
        indice_hhi_concentracao_scm
    from {{ ref('stg_anatel_banda_larga_scm') }}
),

cobertura as (
    select
        id_municipio,
        populacao_coberta_4g_pct,
        populacao_coberta_5g_pct,
        area_geografica_coberta_movel_pct,
        cobertura_rodovias_federais_pct,
        localidades_rurais_conectadas_pct,
        escolas_publicas_conectadas_pct
    from {{ ref('stg_anatel_cobertura_setores') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    e.id_municipio,
    e.nome_municipio,
    e.sigla_uf,
    e.id_uf,
    coalesce(m.nome_uf, e.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    e.ano_mes_referencia,
    e.total_erbs_licenciadas,
    e.erbs_5g_nr,
    e.erbs_4g_lte,
    e.erbs_3g_wcdma,
    e.erbs_2g_gsm,
    round((cast(e.erbs_5g_nr as double) / nullif(e.total_erbs_licenciadas, 0)) * 100.0, 2) as proporcao_erbs_5g_pct,
    e.operadoras_ativas_smp,
    e.operadora_lider_erbs,
    e.densidade_erbs_por_10k_hab,
    e.densidade_erbs_por_100km2,
    e.cobertura_urbana_5g_pct,
    e.cobertura_urbana_4g_pct,
    b.possui_backhaul_fibra,
    b.total_acessos_scm,
    b.acessos_fibra_ftth,
    b.acessos_cabo_coaxial,
    b.acessos_radio_satelite,
    b.participacao_fibra_pct as participacao_fibra_ftth_pct,
    b.velocidade_mediana_mbps,
    b.prestadoras_scm_ativas,
    b.indice_hhi_concentracao_scm,
    c.populacao_coberta_4g_pct,
    c.populacao_coberta_5g_pct,
    c.area_geografica_coberta_movel_pct,
    c.cobertura_rodovias_federais_pct,
    c.localidades_rurais_conectadas_pct,
    c.escolas_publicas_conectadas_pct,
    e.reference_date,
    e.extracted_at
from estacoes e
inner join banda_larga b on e.id_municipio = b.id_municipio
inner join cobertura c on e.id_municipio = c.id_municipio
left join municipios m on e.id_municipio = m.id_municipio
