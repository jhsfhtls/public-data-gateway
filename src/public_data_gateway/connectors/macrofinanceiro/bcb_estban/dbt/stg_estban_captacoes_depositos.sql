{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_estban/silver/estban_captacoes_depositos.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(data_base_yyyymm as varchar) as data_base_yyyymm,
    cast(depositos_vista_brl as double) as depositos_vista_brl,
    cast(depositos_poupanca_brl as double) as depositos_poupanca_brl,
    cast(depositos_prazo_cdb_rdb_brl as double) as depositos_prazo_cdb_rdb_brl,
    cast(depositos_governo_publico_brl as double) as depositos_governo_publico_brl,
    cast(liquidez_imediata_caixa_brl as double) as liquidez_imediata_caixa_brl
from source
