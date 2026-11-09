{{ config(materialized='table') }}

with tipos as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        tipo_edificacao,
        total_edificacoes,
        participacao_pct,
        densidade_edificacoes_km2,
        reference_date,
        extracted_at
    from {{ ref('stg_cnefe_tipos_edificacao') }}
),

densidade as (
    select
        id_municipio,
        total_enderecos_municipio,
        total_logradouros_distintos,
        total_ceps_distintos,
        taxa_cobertura_georreferenciada_pct,
        densidade_enderecos_km2 as densidade_total_enderecos_km2,
        proporcao_enderecos_comerciais_pct
    from {{ ref('stg_cnefe_densidade_enderecamento') }}
)

select
    t.id_municipio,
    t.sigla_uf,
    t.nome_municipio,
    t.tipo_edificacao,
    t.total_edificacoes,
    t.participacao_pct,
    t.densidade_edificacoes_km2,
    d.total_enderecos_municipio,
    d.total_logradouros_distintos,
    d.total_ceps_distintos,
    d.taxa_cobertura_georreferenciada_pct,
    d.densidade_total_enderecos_km2,
    d.proporcao_enderecos_comerciais_pct,
    t.reference_date,
    t.extracted_at
from tipos t
left join densidade d on t.id_municipio = d.id_municipio
