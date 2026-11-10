{{ config(materialized='table') }}

with fct as (
    select * from {{ ref('fct_pib_municipal_pontual') }}
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    regiao,
    is_capital,
    is_amazonia_legal,
    ano,
    valor_pib_mil_reais,
    valor_impostos_liquidos_mil_reais,
    vab_total_mil_reais,
    vab_agropecuaria_mil_reais,
    vab_industria_mil_reais,
    vab_servicos_mil_reais,
    vab_administracao_publica_mil_reais,
    participacao_pib_brasil_pct,
    participacao_pib_uf_pct,
    round((vab_agropecuaria_mil_reais / nullif(vab_total_mil_reais, 0)) * 100.0, 2) as share_vab_agropecuaria_pct,
    round((vab_industria_mil_reais / nullif(vab_total_mil_reais, 0)) * 100.0, 2) as share_vab_industria_pct,
    round((vab_servicos_mil_reais / nullif(vab_total_mil_reais, 0)) * 100.0, 2) as share_vab_servicos_pct,
    round((vab_administracao_publica_mil_reais / nullif(vab_total_mil_reais, 0)) * 100.0, 2) as share_vab_administracao_publica_pct,
    case
        when (vab_agropecuaria_mil_reais / nullif(vab_total_mil_reais, 0)) >= 0.35 then 'AGROPECUARIA'
        when (vab_industria_mil_reais / nullif(vab_total_mil_reais, 0)) >= 0.30 then 'INDUSTRIAL'
        when (vab_administracao_publica_mil_reais / nullif(vab_total_mil_reais, 0)) >= 0.35 then 'ADMINISTRACAO_PUBLICA'
        else 'SERVICOS'
    end as vocacao_economica_predominante,
    case
        when participacao_pib_brasil_pct >= 2.0 then 'CONCENTRACAO_MUITO_ELEVADA'
        when participacao_pib_brasil_pct >= 0.5 then 'CONCENTRACAO_ELEVADA'
        when participacao_pib_brasil_pct >= 0.1 then 'CONCENTRACAO_MODERADA'
        else 'CONCENTRACAO_DESCENTRALIZADA'
    end as grau_concentracao_nacional,
    fonte,
    extracted_at
from fct
