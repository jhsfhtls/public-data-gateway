{{ config(materialized='table') }}

with caged as (
    select * from {{ ref('fct_caged_movimentacao_mensal') }}
),

censo as (
    select
        id_municipio,
        populacao_residente,
        taxa_urbanizacao_pct,
        taxa_alfabetizacao_pct
    from {{ ref('fct_censo_demografico_municipios') }}
),

pib as (
    select
        id_municipio,
        pib_per_capita_reais,
        perfil_economico_dominante
    from {{ ref('mart_perfil_socioeconomico_municipios') }}
)

select
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.regiao,
    c.is_capital,
    c.competencia_movimentacao,
    c.total_admissoes,
    c.total_desligamentos,
    c.saldo_movimentacao,
    c.estoque_declarado_aproximado,
    c.taxa_rotatividade_turnover_pct,
    c.salario_medio_admissao_reais,
    c.salario_medio_desligamento_reais,
    round(
        c.salario_medio_admissao_reais / nullif(c.salario_medio_desligamento_reais, 0),
        4
    ) as razao_salarial_admissao_desligamento,
    c.taxa_crescimento_emprego_pct,
    c.regime_movimentacao,
    coalesce(pop.populacao_residente, 100000) as populacao_residente,
    coalesce(p.pib_per_capita_reais, 35000.0) as pib_per_capita_reais,
    coalesce(p.perfil_economico_dominante, 'SERVICOS') as perfil_economico_dominante,
    round(
        least(
            100.0,
            greatest(
                0.0,
                50.0 + (c.taxa_crescimento_emprego_pct * 40.0) + ((c.salario_medio_admissao_reais / 3000.0) * 15.0)
            )
        ),
        2
    ) as score_aquecimento_mercado_trabalho,
    round(
        least(
            100.0,
            greatest(
                0.0,
                (coalesce(c.taxa_rotatividade_turnover_pct, 3.0) * 8.0)
                + (case when c.saldo_movimentacao < 0 then 20.0 else 5.0 end)
                + (case when c.salario_medio_admissao_reais < 2300.0 then 15.0 else 0.0 end)
            )
        ),
        2
    ) as score_vulnerabilidade_emprego,
    case
        when c.taxa_crescimento_emprego_pct >= 0.5 then 'EXPANSAO_ACELERADA'
        when c.taxa_crescimento_emprego_pct > 0.0 then 'CRESCIMENTO_MODERADO'
        when c.taxa_crescimento_emprego_pct = 0.0 then 'ESTABILIDADE'
        else 'CONTRACAO'
    end as classificacao_dinamica_emprego,
    c.reference_date,
    c.extracted_at
from caged c
left join censo pop on c.id_municipio = pop.id_municipio
left join pib p on c.id_municipio = p.id_municipio
