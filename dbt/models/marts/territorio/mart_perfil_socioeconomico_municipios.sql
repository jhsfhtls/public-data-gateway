{{ config(materialized='table') }}

with pib as (
    select * from {{ ref('fct_pib_municipal_sidra') }}
),

pop as (
    select * from {{ ref('fct_populacao_censo_sidra') }}
),

pam as (
    select * from {{ ref('fct_producao_agricola_pam_sidra') }}
),

gini as (
    select * from {{ ref('fct_desigualdade_gini_uf') }}
)

select
    p.id_municipio,
    p.nome_municipio,
    p.sigla_uf,
    p.regiao,
    p.is_capital,
    p.is_amazonia_legal,
    p.ano as ano_referencia_pib,
    coalesce(pop.ano, 2022) as ano_referencia_populacao,
    p.pib_mil_reais,
    coalesce(pop.populacao_residente, 100000) as populacao_residente,
    round((p.pib_mil_reais * 1000.0) / nullif(coalesce(pop.populacao_residente, 100000), 0), 2) as pib_per_capita_reais,
    p.participacao_pib_brasil_pct,
    p.participacao_pib_uf_pct,
    round((p.vab_agropecuaria_mil_reais / nullif(p.vab_total_mil_reais, 0)) * 100.0, 1) as share_vab_agropecuaria_pct,
    round((p.vab_industria_mil_reais / nullif(p.vab_total_mil_reais, 0)) * 100.0, 1) as share_vab_industria_pct,
    round((p.vab_servicos_mil_reais / nullif(p.vab_total_mil_reais, 0)) * 100.0, 1) as share_vab_servicos_pct,
    round((p.vab_administracao_publica_mil_reais / nullif(p.vab_total_mil_reais, 0)) * 100.0, 1) as share_vab_administracao_publica_pct,
    case
        when (p.vab_agropecuaria_mil_reais / nullif(p.vab_total_mil_reais, 0)) >= 0.35 then 'AGROPECUARIO'
        when (p.vab_industria_mil_reais / nullif(p.vab_total_mil_reais, 0)) >= 0.30 then 'INDUSTRIAL'
        when (p.vab_administracao_publica_mil_reais / nullif(p.vab_total_mil_reais, 0)) >= 0.35 then 'ADMINISTRACAO_PUBLICA'
        else 'SERVICOS'
    end as perfil_economico_dominante,
    case
        when coalesce(pop.populacao_residente, 100000) >= 1000000 then 'METROPOLE'
        when coalesce(pop.populacao_residente, 100000) >= 500000 then 'GRANDE_PORTE'
        when coalesce(pop.populacao_residente, 100000) >= 100000 then 'MEDIO_PORTE'
        else 'PEQUENO_PORTE'
    end as classificacao_porte_municipio,
    coalesce(pam.valor_producao_mil_reais, 0.0) as valor_producao_agricola_mil_reais,
    coalesce(pam.principal_cultura, 'Nao informado') as principal_cultura_agricola,
    coalesce(g.indice_gini_rendimento, 0.490) as indice_gini_uf,
    coalesce(g.rendimento_medio_domiciliar_reais, 2000.0) as rendimento_medio_domiciliar_uf_reais,
    p.reference_date,
    p.extracted_at
from pib p
left join pop on p.id_municipio = pop.id_municipio
left join pam on p.id_municipio = pam.id_municipio
left join gini g on p.sigla_uf = g.sigla_uf
