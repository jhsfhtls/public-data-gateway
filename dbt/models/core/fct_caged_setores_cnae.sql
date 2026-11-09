{{ config(materialized='table') }}

with set_cnae as (
    select * from {{ ref('stg_caged_perfil_setorial_cnae') }}
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
    s.id_municipio,
    coalesce(mun.nome_municipio, s.nome_municipio) as nome_municipio,
    coalesce(mun.sigla_uf, s.sigla_uf) as sigla_uf,
    mun.regiao,
    coalesce(mun.is_capital, false) as is_capital,
    s.competencia_movimentacao,
    s.grande_setor_cnae,
    s.total_admissoes,
    s.total_desligamentos,
    s.saldo_setorial,
    s.salario_medio_admissao_reais,
    s.reference_date,
    s.extracted_at
from set_cnae s
left join mun on s.id_municipio = mun.id_municipio
