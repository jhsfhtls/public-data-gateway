{{ config(materialized='table') }}

with mov as (
    select * from {{ ref('stg_caged_movimentacao_mensal') }}
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
    m.id_municipio,
    coalesce(mun.nome_municipio, m.nome_municipio) as nome_municipio,
    coalesce(mun.sigla_uf, m.sigla_uf) as sigla_uf,
    mun.regiao,
    coalesce(mun.is_capital, false) as is_capital,
    m.competencia_movimentacao,
    m.total_admissoes,
    m.total_desligamentos,
    m.saldo_movimentacao,
    m.estoque_declarado_aproximado,
    m.taxa_rotatividade_turnover_pct,
    m.salario_medio_admissao_reais,
    m.salario_medio_desligamento_reais,
    round(
        (m.saldo_movimentacao * 100.0) / nullif(coalesce(m.estoque_declarado_aproximado, 100000), 0),
        3
    ) as taxa_crescimento_emprego_pct,
    case
        when m.saldo_movimentacao > 0 then 'GERACAO_LIQUIDA'
        when m.saldo_movimentacao < 0 then 'FECHAMENTO_POSTOS'
        else 'ESTABILIDADE'
    end as regime_movimentacao,
    m.reference_date,
    m.extracted_at
from mov m
left join mun on m.id_municipio = mun.id_municipio
