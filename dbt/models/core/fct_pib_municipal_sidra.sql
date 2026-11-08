{{ config(materialized='table') }}

with pib as (
    select * from {{ ref('stg_sidra_pib_municipal') }}
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
    p.id_municipio,
    coalesce(m.nome_municipio, p.nome_municipio) as nome_municipio,
    coalesce(m.sigla_uf, p.sigla_uf) as sigla_uf,
    m.regiao,
    coalesce(m.is_capital, false) as is_capital,
    coalesce(m.is_amazonia_legal, false) as is_amazonia_legal,
    p.ano,
    p.pib_mil_reais,
    p.vab_total_mil_reais,
    p.vab_agropecuaria_mil_reais,
    p.vab_industria_mil_reais,
    p.vab_servicos_mil_reais,
    p.vab_administracao_publica_mil_reais,
    p.impostos_liquidos_mil_reais,
    p.participacao_pib_brasil_pct,
    p.participacao_pib_uf_pct,
    p.reference_date,
    p.extracted_at
from pib p
left join mun m on p.id_municipio = m.id_municipio
