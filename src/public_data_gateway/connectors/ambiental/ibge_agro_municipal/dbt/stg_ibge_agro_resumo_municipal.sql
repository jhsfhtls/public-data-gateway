{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibge_agro_municipal/silver/ibge_agro_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(area_colhida_pam_ha as double) as area_colhida_pam_ha,
    cast(valor_producao_pam_mil_brl as double) as valor_producao_pam_mil_brl,
    cast(efetivo_rebanho_bovino_cabecas as bigint) as efetivo_rebanho_bovino_cabecas,
    cast(valor_producao_pecuaria_mil_brl as double) as valor_producao_pecuaria_mil_brl,
    cast(valor_producao_silvicultura_pevs_mil_brl as double) as valor_producao_silvicultura_pevs_mil_brl,
    cast(valor_bruto_agro_total_mil_brl as double) as valor_bruto_agro_total_mil_brl,
    cast(produtividade_media_pam_kg_ha as double) as produtividade_media_pam_kg_ha
from source
