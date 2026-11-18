{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cno/silver/cno_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(total_obras_cno as bigint) as total_obras_cno,
    cast(obras_ativas_em_andamento as bigint) as obras_ativas_em_andamento,
    cast(obras_paralisadas_suspensas as bigint) as obras_paralisadas_suspensas,
    cast(obras_encerradas_habitese as bigint) as obras_encerradas_habitese,
    cast(area_total_registrada_m2 as double) as area_total_registrada_m2
from source
