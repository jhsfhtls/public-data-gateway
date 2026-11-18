{{ config(materialized='table') }}

with res as (
    select * from {{ ref('stg_ibama_resumo_municipal') }}
),

julg as (
    select * from {{ ref('stg_ibama_julgamentos_multas') }}
),

autos_agg as (
    select
        id_municipio,
        count(*) as qtd_autos_amostra_sifisc,
        round(avg(valor_multa_brl), 2) as ticket_medio_multa_amostra_brl
    from {{ ref('stg_ibama_autos_infracao') }}
    group by id_municipio
),

emb_agg as (
    select
        id_municipio,
        count(*) as qtd_termos_embargo_amostra,
        sum(case when status_embargo = 'Ativo' then 1 else 0 end) as qtd_embargos_ativos_amostra
    from {{ ref('stg_ibama_termos_embargo') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.bioma_predominante,
    r.qtd_autos_infracao_ibama,
    r.qtd_embargos_ativos_ibama,
    r.area_embargada_ibama_ha,
    r.valor_multas_ibama_brl,
    r.taxa_infracoes_flora_pct,
    j.qtd_processos_julgados,
    j.valor_multas_arrecadadas_brl,
    j.percentual_divida_inscrita_pgfn_pct,
    j.indice_recuperacao_embargos_pct,
    coalesce(a.qtd_autos_amostra_sifisc, 0) as qtd_autos_amostra_sifisc,
    coalesce(a.ticket_medio_multa_amostra_brl, 0.0) as ticket_medio_multa_amostra_brl,
    coalesce(e.qtd_termos_embargo_amostra, 0) as qtd_termos_embargo_amostra,
    coalesce(e.qtd_embargos_ativos_amostra, 0) as qtd_embargos_ativos_amostra
from res r
left join julg j on r.id_municipio = j.id_municipio
left join autos_agg a on r.id_municipio = a.id_municipio
left join emb_agg e on r.id_municipio = e.id_municipio
