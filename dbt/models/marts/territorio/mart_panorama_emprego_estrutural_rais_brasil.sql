{{ config(materialized='table') }}

with setor_ranked as (
    select
        ano,
        id_municipio,
        grande_setor_cnae,
        total_vinculos_ativos,
        participacao_vinculos_municipio_pct,
        row_number() over (
            partition by ano, id_municipio
            order by total_vinculos_ativos desc, grande_setor_cnae
        ) as posicao
    from {{ ref('fct_rais_vinculos_setor_municipio') }}
),

estabelecimentos as (
    select ano, id_municipio, sum(total_estabelecimentos) as total_estabelecimentos
    from {{ ref('stg_rais_estabelecimentos_porte') }}
    group by ano, id_municipio
),

demografia as (
    select
        ano,
        id_municipio,
        round(sum(tempo_medio_emprego_meses * total_vinculos_ativos)
            / nullif(sum(total_vinculos_ativos), 0), 2) as tempo_medio_emprego_meses,
        round(sum(taxa_vinculos_ate_12_meses_pct * total_vinculos_ativos)
            / nullif(sum(total_vinculos_ativos), 0), 2) as taxa_vinculos_ate_12_meses_pct
    from {{ ref('stg_rais_rotatividade_demografia') }}
    group by ano, id_municipio
),

gold as (
    select
        ano,
        id_municipio,
        score_formalizacao_trabalho_municipio,
        classificacao_formalizacao_trabalho_municipio
    from read_parquet('data/mte_rais/gold/features_rais_mercado_formal_municipios_brasil.parquet')
)

select
    f.ano,
    f.id_municipio,
    coalesce(geo.nome_municipio, f.nome_municipio) as nome_municipio,
    coalesce(geo.sigla_uf, f.sigla_uf) as sigla_uf,
    geo.regiao,
    geo.is_capital,
    f.total_vinculos_ativos,
    e.total_estabelecimentos,
    f.remuneracao_media_reais,
    f.remuneracao_media_sm,
    f.massa_salarial_anual_reais,
    s.grande_setor_cnae as setor_dominante_cnae,
    s.participacao_vinculos_municipio_pct as participacao_setor_dominante_pct,
    d.tempo_medio_emprego_meses,
    d.taxa_vinculos_ate_12_meses_pct,
    g.score_formalizacao_trabalho_municipio,
    g.classificacao_formalizacao_trabalho_municipio,
    f.source_url,
    f.reference_date,
    f.extracted_at
from {{ ref('fct_rais_mercado_formal_municipio') }} f
left join {{ ref('dim_municipio') }} geo on f.id_municipio = geo.id_municipio
left join estabelecimentos e on f.ano = e.ano and f.id_municipio = e.id_municipio
left join setor_ranked s on f.ano = s.ano and f.id_municipio = s.id_municipio and s.posicao = 1
left join demografia d on f.ano = d.ano and f.id_municipio = d.id_municipio
left join gold g on f.ano = g.ano and f.id_municipio = g.id_municipio
