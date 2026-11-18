{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_siconfi/silver/siconfi_extratos_entregas.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_ente as varchar) as nome_ente,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(demonstrativo as varchar) as demonstrativo,
    cast(periodicidade as varchar) as periodicidade,
    cast(status_entrega as varchar) as status_entrega,
    cast(data_homologacao as varchar) as data_homologacao
from source
