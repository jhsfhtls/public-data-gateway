{{ config(materialized='table') }}

with ipca as (
    select * from {{ ref('fct_ipca_mensal_sidra') }}
),

gini_brasil as (
    select
        ano,
        round(avg(indice_gini_rendimento), 3) as media_gini_nacional,
        round(avg(rendimento_medio_domiciliar_reais), 2) as media_rendimento_domiciliar_nacional
    from {{ ref('fct_desigualdade_gini_uf') }}
    group by ano
)

select
    i.ano_mes,
    i.ano,
    i.mes,
    i.mes_ano_extenso,
    i.ipca_variacao_mensal_pct,
    i.ipca_acumulado_ano_pct,
    i.ipca_acumulado_12m_pct,
    i.regime_inflacionario_mensal,
    coalesce(g.media_gini_nacional, 0.490) as media_gini_nacional,
    coalesce(g.media_rendimento_domiciliar_nacional, 2000.0) as media_rendimento_domiciliar_nacional_reais,
    case
        when i.ipca_acumulado_12m_pct > 6.0 and coalesce(g.media_gini_nacional, 0.490) >= 0.50 then 'ALTO_RISCO_SOCIAL'
        when i.ipca_acumulado_12m_pct > 4.5 then 'ALERTA_PODER_COMPRA'
        else 'ESTABILIDADE_MACROECONOMICA'
    end as classificacao_clima_macroeconomico,
    i.reference_date,
    i.extracted_at
from ipca i
left join gini_brasil g on i.ano = g.ano
