{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(ano_colecao as bigint) as ano_colecao,
    cast(area_total_municipio_ha as double) as area_total_municipio_ha,
    cast(area_floresta_nativa_ha as double) as area_floresta_nativa_ha,
    cast(area_formacao_campestre_savana_ha as double) as area_formacao_campestre_savana_ha,
    cast(area_agropecuaria_total_ha as double) as area_agropecuaria_total_ha,
    cast(area_nao_vegetada_urbana_ha as double) as area_nao_vegetada_urbana_ha,
    cast(area_corpos_dagua_ha as double) as area_corpos_dagua_ha,
    cast(cobertura_nativa_pct as double) as cobertura_nativa_pct,
    cast(indice_preservacao_nativa as double) as indice_preservacao_nativa,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapbiomas_cobertura/silver/mapbiomas_cobertura_anual_municipio.parquet')
