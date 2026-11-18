{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_agencia/silver/agencias_cidades_sem_atendimento.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(possui_agencia_fisica as boolean) as possui_agencia_fisica,
    cast(dependencia_correspondente_bancario_pct as double) as dependencia_correspondente_bancario_pct,
    cast(distancia_media_agencia_km as double) as distancia_media_agencia_km,
    cast(status_inclusao_financeira as varchar) as status_inclusao_financeira
from source
