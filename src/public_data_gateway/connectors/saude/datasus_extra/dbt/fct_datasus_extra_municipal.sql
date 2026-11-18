{{ config(materialized='table') }}

with sia as (
    select
        id_municipio,
        count(*) as qtd_linhas_procedimento_sia,
        round(sum(valor_aprovado_sia_mil_brl), 2) as valor_amostra_sia_mil_brl
    from {{ ref('stg_datasus_sia_producao_ambulatorial') }}
    group by id_municipio
),

sinan as (
    select
        id_municipio,
        notificacoes_dengue_12m,
        notificacoes_respiratorias_12m,
        taxa_incidencia_100k_hab,
        oportunidade_encerramento_pct
    from {{ ref('stg_datasus_sinan_agravos_notificacao') }}
),

sisvan as (
    select
        id_municipio,
        prevalencia_obesidade_adulto_pns_pct,
        acompanhamento_condicoes_cronicas_pct,
        inseguranca_alimentar_grave_pct
    from {{ ref('stg_datasus_sisvan_pns_nutricao') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.producao_sia_total_12m,
    r.notificacoes_sinan_total_12m,
    r.valor_sia_aprovado_mil_brl,
    r.cobertura_sisvan_pct,
    r.atendimentos_sia_per_capita,
    coalesce(si.qtd_linhas_procedimento_sia, 2) as qtd_linhas_procedimento_sia,
    coalesce(si.valor_amostra_sia_mil_brl, 0.0) as valor_amostra_sia_mil_brl,
    coalesce(sn.notificacoes_dengue_12m, 0) as notificacoes_dengue_12m,
    coalesce(sn.notificacoes_respiratorias_12m, 0) as notificacoes_respiratorias_12m,
    coalesce(sn.taxa_incidencia_100k_hab, 0.0) as taxa_incidencia_100k_hab,
    coalesce(sn.oportunidade_encerramento_pct, 85.0) as oportunidade_encerramento_pct,
    coalesce(sv.prevalencia_obesidade_adulto_pns_pct, 22.0) as prevalencia_obesidade_adulto_pns_pct,
    coalesce(sv.acompanhamento_condicoes_cronicas_pct, 75.0) as acompanhamento_condicoes_cronicas_pct,
    coalesce(sv.inseguranca_alimentar_grave_pct, 5.0) as inseguranca_alimentar_grave_pct
from {{ ref('stg_datasus_extra_resumo_municipal') }} r
left join sia si on r.id_municipio = si.id_municipio
left join sinan sn on r.id_municipio = sn.id_municipio
left join sisvan sv on r.id_municipio = sv.id_municipio
