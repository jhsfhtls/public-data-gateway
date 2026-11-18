{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cafir/silver/cafir_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(qtd_imoveis_cafir as bigint) as qtd_imoveis_cafir,
    cast(area_total_cafir_ha as double) as area_total_cafir_ha,
    cast(area_media_imovel_cafir_ha as double) as area_media_imovel_cafir_ha,
    cast(taxa_vinculacao_cnir_pct as double) as taxa_vinculacao_cnir_pct,
    cast(vtn_medio_estimado_brl_ha as double) as vtn_medio_estimado_brl_ha
from source
