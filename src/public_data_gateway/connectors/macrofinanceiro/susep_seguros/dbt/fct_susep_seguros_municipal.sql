{{ config(materialized='table') }}

with ramos as (
    select
        id_municipio,
        count(*) as qtd_ramos_seguro_susep,
        round(sum(premio_direto_anual_mil_brl), 2) as premios_amostra_ramos_mil_brl
    from {{ ref('stg_susep_premios_sinistros_ramo') }}
    group by id_municipio
),

prev as (
    select
        id_municipio,
        contribuicoes_previdencia_mil_brl,
        resgates_pagamentos_mil_brl,
        provisoes_tecnicas_mi_brl,
        participacao_vgbl_pct
    from {{ ref('stg_susep_previdencia_vgbl_pgbl_uf') }}
),

corr as (
    select
        id_municipio,
        agentes_corretor_pj,
        indice_solvencia_regulatoria_pct,
        indice_cobertura_provisoes_pct
    from {{ ref('stg_susep_corretoras_solvencia_mercado') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.premios_emitidos_total_mil_brl,
    r.sinistros_ocorridos_mil_brl,
    r.taxa_sinistralidade_media_pct,
    r.corretoras_ativas_susep,
    r.premio_per_capita_brl,
    coalesce(rm.qtd_ramos_seguro_susep, 2) as qtd_ramos_seguro_susep,
    coalesce(rm.premios_amostra_ramos_mil_brl, 0.0) as premios_amostra_ramos_mil_brl,
    coalesce(p.contribuicoes_previdencia_mil_brl, 0.0) as contribuicoes_previdencia_mil_brl,
    coalesce(p.resgates_pagamentos_mil_brl, 0.0) as resgates_pagamentos_mil_brl,
    coalesce(p.provisoes_tecnicas_mi_brl, 0.0) as provisoes_tecnicas_mi_brl,
    coalesce(p.participacao_vgbl_pct, 75.0) as participacao_vgbl_pct,
    coalesce(c.agentes_corretor_pj, 0) as agentes_corretor_pj,
    coalesce(c.indice_solvencia_regulatoria_pct, 150.0) as indice_solvencia_regulatoria_pct,
    coalesce(c.indice_cobertura_provisoes_pct, 130.0) as indice_cobertura_provisoes_pct
from {{ ref('stg_susep_seguros_resumo_municipal') }} r
left join ramos rm on r.id_municipio = rm.id_municipio
left join prev p on r.id_municipio = p.id_municipio
left join corr c on r.id_municipio = c.id_municipio
