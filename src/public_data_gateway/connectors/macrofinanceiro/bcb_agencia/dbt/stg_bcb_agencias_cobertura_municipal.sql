{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_agencia/silver/agencias_cobertura_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(total_agencias_fisicas as bigint) as total_agencias_fisicas,
    cast(total_bancos_presentes as bigint) as total_bancos_presentes,
    cast(agencias_por_100k_hab as double) as agencias_por_100k_hab,
    cast(percentual_bancos_publicos_pct as double) as percentual_bancos_publicos_pct,
    cast(percentual_bancos_privados_pct as double) as percentual_bancos_privados_pct
from source
