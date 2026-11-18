{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_siconfi/silver/siconfi_dca_balanco.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_ente as varchar) as nome_ente,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(ativo_total_brl as double) as ativo_total_brl,
    cast(passivo_total_brl as double) as passivo_total_brl,
    cast(patrimonio_liquido_brl as double) as patrimonio_liquido_brl,
    cast(receita_orcamentaria_realizada_brl as double) as receita_orcamentaria_realizada_brl,
    cast(despesa_orcamentaria_empenhada_brl as double) as despesa_orcamentaria_empenhada_brl,
    cast(resultado_orcamentario_brl as double) as resultado_orcamentario_brl
from source
