{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cafir/silver/cafir_vinculos_cnir_sncr.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(taxa_vinculacao_cnir_pct as double) as taxa_vinculacao_cnir_pct,
    cast(taxa_georreferenciamento_sigef_pct as double) as taxa_georreferenciamento_sigef_pct,
    cast(paridade_cib_car_pct as double) as paridade_cib_car_pct,
    cast(imoveis_pendentes_vinculacao_cnt as bigint) as imoveis_pendentes_vinculacao_cnt
from source
