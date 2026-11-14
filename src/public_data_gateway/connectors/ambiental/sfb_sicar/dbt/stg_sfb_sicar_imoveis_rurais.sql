{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(qtd_imoveis_car_ativos as bigint) as qtd_imoveis_car_ativos,
    cast(area_cadastrada_car_ha as double) as area_cadastrada_car_ha,
    cast(area_media_imovel_ha as double) as area_media_imovel_ha,
    cast(imoveis_analise_concluida_pct as double) as imoveis_analise_concluida_pct,
    cast(indice_cobertura_cadastral_car as double) as indice_cobertura_cadastral_car,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/sfb_sicar/silver/sicar_imoveis_rurais_municipio.parquet')
