{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_estban/silver/estban_instituicoes_municipio.parquet')
)

select
    cast(id_registro_estban as varchar) as id_registro_estban,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(cnpj_8_if as varchar) as cnpj_8_if,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(nome_instituicao as varchar) as nome_instituicao,
    cast(qtd_agencias_municipio as bigint) as qtd_agencias_municipio,
    cast(saldo_credito_if_municipio_brl as double) as saldo_credito_if_municipio_brl,
    cast(saldo_depositos_if_municipio_brl as double) as saldo_depositos_if_municipio_brl,
    cast(share_credito_local_pct as double) as share_credito_local_pct
from source
