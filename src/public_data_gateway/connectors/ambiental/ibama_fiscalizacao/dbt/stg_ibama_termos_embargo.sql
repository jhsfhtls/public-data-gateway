{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibama_fiscalizacao/silver/ibama_termos_embargo.parquet')
)

select
    cast(num_tad_embargo as varchar) as num_tad_embargo,
    cast(num_auto_infracao as varchar) as num_auto_infracao,
    cast(documento_embargado as varchar) as documento_embargado,
    cast(nome_embargado as varchar) as nome_embargado,
    cast(area_embargada_ha as double) as area_embargada_ha,
    cast(bioma as varchar) as bioma,
    cast(status_embargo as varchar) as status_embargo,
    cast(imovel_car_vinculado as varchar) as imovel_car_vinculado,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(data_embargo as varchar) as data_embargo
from source
