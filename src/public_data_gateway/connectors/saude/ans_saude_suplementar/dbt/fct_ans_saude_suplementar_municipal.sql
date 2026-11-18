{{ config(materialized='table') }}

with ben as (
    select
        id_municipio,
        count(*) as qtd_segmentacoes_ans
    from {{ ref('stg_ans_beneficiarios_municipio') }}
    group by id_municipio
),

idss as (
    select
        id_municipio,
        operadoras_atuantes_count,
        ticket_medio_mensal_brl,
        reclamacoes_nip_10k_benef
    from {{ ref('stg_ans_operadoras_idss_qualidade') }}
),

diops as (
    select
        id_municipio,
        provisoes_tecnicas_cobertura_pct,
        indice_solvencia_ans_pct
    from {{ ref('stg_ans_sinistralidade_diops_economico') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.total_beneficiarios_med_odo,
    r.taxa_cobertura_privada_pct,
    r.idss_medio_ponderado,
    r.sinistralidade_assistencial_pct,
    r.receita_contraprestacoes_mil_brl,
    coalesce(b.qtd_segmentacoes_ans, 2) as qtd_segmentacoes_ans,
    coalesce(q.operadoras_atuantes_count, 35) as operadoras_atuantes_count,
    coalesce(q.ticket_medio_mensal_brl, 680.0) as ticket_medio_mensal_brl,
    coalesce(q.reclamacoes_nip_10k_benef, 3.5) as reclamacoes_nip_10k_benef,
    coalesce(d.provisoes_tecnicas_cobertura_pct, 105.0) as provisoes_tecnicas_cobertura_pct,
    coalesce(d.indice_solvencia_ans_pct, 112.0) as indice_solvencia_ans_pct
from {{ ref('stg_ans_saude_suplementar_resumo_municipal') }} r
left join ben b on r.id_municipio = b.id_municipio
left join idss q on r.id_municipio = q.id_municipio
left join diops d on r.id_municipio = d.id_municipio
