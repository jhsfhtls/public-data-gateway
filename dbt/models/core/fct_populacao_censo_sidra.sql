{{ config(materialized='table') }}

with pop as (
    select * from {{ ref('stg_sidra_populacao_censo') }}
),

mun as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        is_capital
    from {{ ref('dim_municipio') }}
)

select
    p.id_municipio,
    coalesce(m.nome_municipio, p.nome_municipio) as nome_municipio,
    coalesce(m.sigla_uf, p.sigla_uf) as sigla_uf,
    m.regiao,
    coalesce(m.is_capital, false) as is_capital,
    p.ano,
    p.populacao_residente,
    p.unidade_medida,
    p.densidade_demografica_hab_km2,
    p.reference_date,
    p.extracted_at
from pop p
left join mun m on p.id_municipio = m.id_municipio
