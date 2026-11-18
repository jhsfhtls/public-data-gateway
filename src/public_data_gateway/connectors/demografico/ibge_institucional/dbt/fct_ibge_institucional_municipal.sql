{{ config(materialized='table') }}

with munic as (
    select
        id_municipio,
        count(*) as qtd_eixos_pesquisa_ibge,
        round(avg(indice_capacidade_institucional_munic), 2) as media_indice_institucional_munic
    from {{ ref('stg_ibge_munic_governanca_municipal') }}
    group by id_municipio
),

estadic as (
    select
        id_municipio,
        efetivo_estatutario_estadual_uf,
        politicas_habitacao_saneamento_ativas,
        cobertura_sistema_protecao_civil_pct
    from {{ ref('stg_ibge_estadic_capacidade_estadual') }}
),

pof as (
    select
        id_municipio,
        despesa_mensal_familiar_pof_brl,
        renda_monetaria_media_domiciliar_brl,
        participacao_alimentacao_domicilio_pct
    from {{ ref('stg_ibge_pof_orcamentos_consumo') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.indice_capacidade_institucional_munic,
    r.servidores_estatutarios_municipio,
    r.indice_governanca_estadual_estadic,
    r.despesa_familiar_media_pof_brl,
    r.comprometimento_habitacao_transporte_pct,
    coalesce(mn.qtd_eixos_pesquisa_ibge, 2) as qtd_eixos_pesquisa_ibge,
    coalesce(mn.media_indice_institucional_munic, 80.0) as media_indice_institucional_munic,
    coalesce(es.efetivo_estatutario_estadual_uf, 100000) as efetivo_estatutario_estadual_uf,
    coalesce(es.politicas_habitacao_saneamento_ativas, 20) as politicas_habitacao_saneamento_ativas,
    coalesce(es.cobertura_sistema_protecao_civil_pct, 85.0) as cobertura_sistema_protecao_civil_pct,
    coalesce(pf.despesa_mensal_familiar_pof_brl, r.despesa_familiar_media_pof_brl) as despesa_mensal_familiar_pof_brl,
    coalesce(pf.renda_monetaria_media_domiciliar_brl, 6500.0) as renda_monetaria_media_domiciliar_brl,
    coalesce(pf.participacao_alimentacao_domicilio_pct, 17.5) as participacao_alimentacao_domicilio_pct
from {{ ref('stg_ibge_institucional_resumo_municipal') }} r
left join munic mn on r.id_municipio = mn.id_municipio
left join estadic es on r.id_municipio = es.id_municipio
left join pof pf on r.id_municipio = pf.id_municipio
