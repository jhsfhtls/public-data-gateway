{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_capag/silver/capag_ifgf_gestao_fiscal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(ifgf_geral as double) as ifgf_geral,
    cast(ifgf_autonomia as double) as ifgf_autonomia,
    cast(ifgf_gastos_pessoal as double) as ifgf_gastos_pessoal,
    cast(ifgf_liquidez as double) as ifgf_liquidez,
    cast(ifgf_investimentos as double) as ifgf_investimentos,
    cast(conceito_ifgf as varchar) as conceito_ifgf
from source
