{{ config(materialized='table') }}

with pam as (
    select * from {{ ref('stg_sidra_agropecuaria_pam') }}
),

mun as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    a.id_municipio,
    coalesce(m.nome_municipio, a.nome_municipio) as nome_municipio,
    coalesce(m.sigla_uf, a.sigla_uf) as sigla_uf,
    m.regiao,
    a.ano,
    a.quantidade_produzida_toneladas,
    a.valor_producao_mil_reais,
    a.area_plantada_hectares,
    a.area_colhida_hectares,
    a.principal_cultura,
    a.reference_date,
    a.extracted_at
from pam a
left join mun m on a.id_municipio = m.id_municipio
