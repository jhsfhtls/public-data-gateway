{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_tesouro_transparente/silver/stn_transferencias_fpe.parquet')
)

select
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_uf as varchar) as nome_uf,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(mes_competencia as bigint) as mes_competencia,
    cast(decendio as bigint) as decendio,
    cast(valor_bruto_fpe as double) as valor_bruto_fpe,
    cast(deducao_fundeb as double) as deducao_fundeb,
    cast(valor_liquido_fpe as double) as valor_liquido_fpe
from source
