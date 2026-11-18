{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibge_agro_municipal/silver/ibge_pevs_silvicultura.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(area_floresta_plantada_eucalipto_pinus_ha as double) as area_floresta_plantada_eucalipto_pinus_ha,
    cast(producao_madeira_tora_m3 as double) as producao_madeira_tora_m3,
    cast(valor_extracao_vegetal_mil_brl as double) as valor_extracao_vegetal_mil_brl,
    cast(valor_silvicultura_mil_brl as double) as valor_silvicultura_mil_brl
from source
