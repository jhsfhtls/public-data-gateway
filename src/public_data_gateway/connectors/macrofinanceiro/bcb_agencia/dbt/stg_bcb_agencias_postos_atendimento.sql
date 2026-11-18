{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_agencia/silver/agencias_postos_atendimento.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_postos_pa as bigint) as qtd_postos_pa,
    cast(qtd_postos_pae_eletronico as bigint) as qtd_postos_pae_eletronico,
    cast(qtd_unidades_administrativas as bigint) as qtd_unidades_administrativas,
    cast(cobertura_autoatendimento_24h_pct as double) as cobertura_autoatendimento_24h_pct
from source
