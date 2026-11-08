{{ config(materialized='table') }}

with div as (
    select * from {{ ref('stg_censo_diversidade_quilombola_indigena') }}
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
    d.id_municipio,
    coalesce(m.nome_municipio, d.nome_municipio) as nome_municipio,
    coalesce(m.sigla_uf, d.sigla_uf) as sigla_uf,
    m.regiao,
    coalesce(m.is_capital, false) as is_capital,
    d.ano_censo,
    d.populacao_indigena,
    d.populacao_quilombola,
    d.percentual_indigena_pct,
    d.percentual_quilombola_pct,
    d.reference_date,
    d.extracted_at
from div d
left join mun m on d.id_municipio = m.id_municipio
