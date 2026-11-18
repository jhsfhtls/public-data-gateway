{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/cnj_improbidade/silver/cnciai_ressarcimento_erario.parquet')
)

select
    cast(id_condenacao as varchar) as id_condenacao,
    cast(documento_requerido as varchar) as documento_requerido,
    cast(nome_requerido as varchar) as nome_requerido,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(valor_ressarcimento_brl as double) as valor_ressarcimento_brl,
    cast(valor_multa_civil_brl as double) as valor_multa_civil_brl,
    cast(total_condenacao_financeira_brl as double) as total_condenacao_financeira_brl
from source
