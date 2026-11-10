{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_ibge_pib_municipal') }}
),

mun as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        is_capital,
        is_amazonia_legal
    from {{ ref('dim_municipio') }}
)

select
    s.id_municipio,
    coalesce(m.nome_municipio, s.nome_municipio) as nome_municipio,
    coalesce(m.sigla_uf, s.sigla_uf) as sigla_uf,
    coalesce(m.regiao, 'Sudeste') as regiao,
    coalesce(m.is_capital, false) as is_capital,
    coalesce(m.is_amazonia_legal, false) as is_amazonia_legal,
    s.ano,
    s.valor_pib_mil_reais,
    s.valor_impostos_liquidos_mil_reais,
    s.vab_total_mil_reais,
    s.vab_agropecuaria_mil_reais,
    s.vab_industria_mil_reais,
    s.vab_servicos_mil_reais,
    s.vab_administracao_publica_mil_reais,
    s.participacao_pib_brasil_pct,
    s.participacao_pib_uf_pct,
    s.fonte,
    s.extracted_at
from stg s
left join mun m on s.id_municipio = m.id_municipio
