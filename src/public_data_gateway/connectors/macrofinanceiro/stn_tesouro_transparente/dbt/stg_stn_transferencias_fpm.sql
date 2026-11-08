{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_tesouro_transparente/silver/stn_transferencias_fpm.parquet')
)

select
    cast(id_municipio_ibge_7d as varchar) as id_municipio_ibge_7d,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(mes_competencia as bigint) as mes_competencia,
    cast(decendio as bigint) as decendio,
    cast(valor_bruto_fpm as double) as valor_bruto_fpm,
    cast(deducao_fundeb as double) as deducao_fundeb,
    cast(deducao_saude as double) as deducao_saude,
    cast(valor_liquido_fpm as double) as valor_liquido_fpm
from source
