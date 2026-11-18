{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_siconfi/silver/siconfi_entes.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_ente as varchar) as nome_ente,
    cast(esfera as varchar) as esfera,
    cast(regiao as varchar) as regiao,
    cast(populacao as bigint) as populacao,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(is_capital as boolean) as is_capital
from source
