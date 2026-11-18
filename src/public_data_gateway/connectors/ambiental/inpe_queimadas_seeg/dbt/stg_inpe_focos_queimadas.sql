{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/inpe_queimadas_seeg/silver/inpe_focos_queimadas.parquet')
)

select
    cast(cod_foco_inpe as varchar) as cod_foco_inpe,
    cast(satelite_referencia as varchar) as satelite_referencia,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma as varchar) as bioma,
    cast(data_hora_gmt as varchar) as data_hora_gmt,
    cast(risco_fogo as double) as risco_fogo,
    cast(frp_mw as double) as frp_mw,
    cast(dias_sem_chuva as bigint) as dias_sem_chuva,
    cast(precipitacao_mm as double) as precipitacao_mm,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude
from source
