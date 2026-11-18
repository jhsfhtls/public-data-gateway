{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/pgfn_divida_ativa/silver/pgfn_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_trimestre_ref as varchar) as ano_trimestre_ref,
    cast(total_inscricoes_ativas as bigint) as total_inscricoes_ativas,
    cast(total_devedores_pj as bigint) as total_devedores_pj,
    cast(valor_total_geral_brl as double) as valor_total_geral_brl,
    cast(valor_total_previdenciario_brl as double) as valor_total_previdenciario_brl,
    cast(valor_total_fgts_brl as double) as valor_total_fgts_brl,
    cast(valor_total_consolidado_brl as double) as valor_total_consolidado_brl,
    cast(taxa_ajuizamento_pct as double) as taxa_ajuizamento_pct
from source
