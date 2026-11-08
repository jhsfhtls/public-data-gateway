with bal as (
    select * from {{ ref('fct_balanco_bancario_trimestral') }}
),
dre as (
    select * from {{ ref('fct_resultado_bancario_trimestral') }}
),
totais as (
    select
        data_base,
        sum(ativo_total) as ativo_sistema_total,
        sum(carteira_credito_classificada) as credito_sistema_total,
        sum(depositos_totais) as depositos_sistema_total
    from bal
    group by data_base
)

select
    b.cnpj_14_lider,
    b.cod_inst,
    b.nome_instituicao,
    b.segmento_prudencial,
    b.data_base,
    b.ativo_total,
    b.carteira_credito_classificada,
    b.depositos_totais,
    b.patrimonio_liquido,
    d.lucro_liquido_trimestral,
    cast(round((b.ativo_total / nullif(t.ativo_sistema_total, 0)) * 100.0, 2) as double) as market_share_ativo_pct,
    cast(round((b.carteira_credito_classificada / nullif(t.credito_sistema_total, 0)) * 100.0, 2) as double) as market_share_credito_pct,
    dense_rank() over (partition by b.data_base order by b.ativo_total desc) as ranking_ativo_total
from bal b
inner join totais t on b.data_base = t.data_base
left join dre d on b.cod_inst = d.cod_inst and b.data_base = d.data_base
