with gest as (
    select * from {{ ref('dim_gestor_fundo') }}
),
totais as (
    select
        sum(patrimonio_liquido_total) as aum_sistema_total
    from gest
)

select
    g.cnpj_gestor,
    g.nome_gestor,
    g.total_fundos_geridos,
    g.patrimonio_liquido_total as aum_total,
    g.captacao_liquida_recente_total,
    g.principais_classes,
    cast(round((g.patrimonio_liquido_total / nullif(t.aum_sistema_total, 0)) * 100.0, 2) as double) as market_share_aum_pct,
    dense_rank() over (order by g.patrimonio_liquido_total desc) as ranking_aum
from gest g
cross join totais t
