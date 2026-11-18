{{ config(materialized='table') }}

with resumo as (
    select * from {{ ref('stg_pgfn_resumo_municipal') }}
),

gerais as (
    select
        id_municipio,
        count(*) as qtd_inscricoes_gerais_amostra,
        sum(valor_consolidado_brl) as soma_gerais_amostra_brl
    from {{ ref('stg_pgfn_inscricoes_gerais') }}
    group by id_municipio
),

prev as (
    select
        id_municipio,
        count(*) as qtd_inscricoes_prev_amostra,
        sum(valor_consolidado_brl) as soma_prev_amostra_brl
    from {{ ref('stg_pgfn_divida_previdenciaria') }}
    group by id_municipio
),

fgts as (
    select
        id_municipio,
        count(*) as qtd_inscricoes_fgts_amostra,
        sum(valor_consolidado_brl) as soma_fgts_amostra_brl
    from {{ ref('stg_pgfn_divida_fgts') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.ano_trimestre_ref,
    r.total_inscricoes_ativas,
    r.total_devedores_pj,
    r.valor_total_geral_brl,
    r.valor_total_previdenciario_brl,
    r.valor_total_fgts_brl,
    r.valor_total_consolidado_brl,
    r.taxa_ajuizamento_pct,
    coalesce(g.qtd_inscricoes_gerais_amostra, 0) as qtd_inscricoes_gerais_amostra,
    coalesce(p.qtd_inscricoes_prev_amostra, 0) as qtd_inscricoes_prev_amostra,
    coalesce(f.qtd_inscricoes_fgts_amostra, 0) as qtd_inscricoes_fgts_amostra,
    round(coalesce(g.soma_gerais_amostra_brl, 0.0) + coalesce(p.soma_prev_amostra_brl, 0.0) + coalesce(f.soma_fgts_amostra_brl, 0.0), 2) as soma_auditada_amostra_brl
from resumo r
left join gerais g on r.id_municipio = g.id_municipio
left join prev p on r.id_municipio = p.id_municipio
left join fgts f on r.id_municipio = f.id_municipio
