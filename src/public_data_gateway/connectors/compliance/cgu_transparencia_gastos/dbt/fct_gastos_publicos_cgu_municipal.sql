{{ config(materialized='table') }}

with desp as (
    select
        id_municipio,
        count(*) as qtd_modalidades_despesa_cgu,
        round(sum(valor_empenhado_mil_brl), 2) as total_empenhado_cgu_mil_brl
    from {{ ref('stg_cgu_gastos_diretos_orgaos') }}
    group by id_municipio
),

trf as (
    select
        id_municipio,
        repasse_fpm_fundeb_mil_brl,
        repasse_sus_assistencia_mil_brl,
        dependencia_repasses_federais_pct
    from {{ ref('stg_cgu_transferencias_constitucionais_fpm') }}
),

em as (
    select
        id_municipio,
        qtd_convenios_ativos,
        emendas_parlamentares_pagas_mil_brl,
        adimplencia_prestacao_contas_pct,
        uso_cpgf_cartao_mil_brl
    from {{ ref('stg_cgu_emendas_convenios_municipio') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.total_gastos_federais_mil_brl,
    r.total_transferencias_mil_brl,
    r.taxa_execucao_orcamentaria_pct,
    r.gasto_federal_per_capita_brl,
    coalesce(d.qtd_modalidades_despesa_cgu, 0) as qtd_modalidades_despesa_cgu,
    coalesce(d.total_empenhado_cgu_mil_brl, 0.0) as total_empenhado_cgu_mil_brl,
    coalesce(t.repasse_fpm_fundeb_mil_brl, 0.0) as repasse_fpm_fundeb_mil_brl,
    coalesce(t.repasse_sus_assistencia_mil_brl, 0.0) as repasse_sus_assistencia_mil_brl,
    coalesce(t.dependencia_repasses_federais_pct, 25.0) as dependencia_repasses_federais_pct,
    coalesce(e.qtd_convenios_ativos, 0) as qtd_convenios_ativos,
    coalesce(e.emendas_parlamentares_pagas_mil_brl, 0.0) as emendas_parlamentares_pagas_mil_brl,
    coalesce(e.adimplencia_prestacao_contas_pct, 88.0) as adimplencia_prestacao_contas_pct,
    coalesce(e.uso_cpgf_cartao_mil_brl, 0.0) as uso_cpgf_cartao_mil_brl
from {{ ref('stg_cgu_transparencia_resumo_municipal') }} r
left join desp d on r.id_municipio = d.id_municipio
left join trf t on r.id_municipio = t.id_municipio
left join em e on r.id_municipio = e.id_municipio
