{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cno/silver/cno_areas_destinacao.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(area_residencial_m2 as double) as area_residencial_m2,
    cast(area_comercial_industrial_m2 as double) as area_comercial_industrial_m2,
    cast(area_infraestrutura_publica_m2 as double) as area_infraestrutura_publica_m2,
    cast(area_total_construida_m2 as double) as area_total_construida_m2,
    cast(percentual_obras_ativas_pct as double) as percentual_obras_ativas_pct
from source
