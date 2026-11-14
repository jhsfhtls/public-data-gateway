{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(area_pastagem_ha as double) as area_pastagem_ha,
    cast(area_agricultura_temporaria_ha as double) as area_agricultura_temporaria_ha,
    cast(area_agricultura_perene_silvicultura_ha as double) as area_agricultura_perene_silvicultura_ha,
    cast(pastagem_degradada_pct as double) as pastagem_degradada_pct,
    cast(area_irrigada_ha as double) as area_irrigada_ha,
    cast(indice_sustentabilidade_agro as double) as indice_sustentabilidade_agro,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapbiomas_cobertura/silver/mapbiomas_agropecuaria_pastagem_irrigacao.parquet')
