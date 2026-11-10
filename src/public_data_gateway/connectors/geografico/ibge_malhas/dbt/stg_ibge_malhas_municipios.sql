{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_versao as integer) as ano_versao,
    cast(tipo_geometria as varchar) as tipo_geometria,
    cast(geojson_geometry as varchar) as geojson_geometry,
    cast(bbox_min_lon as double) as bbox_min_lon,
    cast(bbox_min_lat as double) as bbox_min_lat,
    cast(bbox_max_lon as double) as bbox_max_lon,
    cast(bbox_max_lat as double) as bbox_max_lat,
    cast(centroide_lon as double) as centroide_lon,
    cast(centroide_lat as double) as centroide_lat,
    cast(qtd_vertices as integer) as qtd_vertices,
    cast(qtd_poligonos as integer) as qtd_poligonos,
    cast(area_aproximada_km2 as double) as area_aproximada_km2
from {{ source('ibge_malhas_silver', 'malhas_municipios') }}
