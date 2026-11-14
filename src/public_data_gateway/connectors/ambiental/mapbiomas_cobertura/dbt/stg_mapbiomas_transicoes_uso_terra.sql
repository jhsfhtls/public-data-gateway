{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(classe_transicao_mapbiomas as varchar) as classe_transicao_mapbiomas,
    cast(area_transicao_ha as double) as area_transicao_ha,
    cast(participacao_transicao_pct as double) as participacao_transicao_pct,
    cast(taxa_conversao_liquida_pct as double) as taxa_conversao_liquida_pct,
    cast(intensidade_pressao_transicao_pct as double) as intensidade_pressao_transicao_pct,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapbiomas_cobertura/silver/mapbiomas_transicoes_uso_terra_classe.parquet')
