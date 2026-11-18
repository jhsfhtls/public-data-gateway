{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/cnj_improbidade/silver/cnciai_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(total_condenacoes_ativas as bigint) as total_condenacoes_ativas,
    cast(total_proibidos_contratar_pj as bigint) as total_proibidos_contratar_pj,
    cast(total_suspensao_direitos_politicos as bigint) as total_suspensao_direitos_politicos,
    cast(valor_total_ressarcimento_multa_brl as double) as valor_total_ressarcimento_multa_brl,
    cast(taxa_proibicao_contratar_pct as double) as taxa_proibicao_contratar_pct
from source
