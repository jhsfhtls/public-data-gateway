{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cafir/silver/cafir_imoveis_rurais.parquet')
)

select
    cast(nirf_cib as varchar) as nirf_cib,
    cast(codigo_incra_sncr as varchar) as codigo_incra_sncr,
    cast(documento_titular as varchar) as documento_titular,
    cast(nome_titular as varchar) as nome_titular,
    cast(nome_imovel_rural as varchar) as nome_imovel_rural,
    cast(situacao_cafir as varchar) as situacao_cafir,
    cast(forma_exploracao as varchar) as forma_exploracao,
    cast(area_total_ha as double) as area_total_ha,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(data_atualizacao as varchar) as data_atualizacao
from source
