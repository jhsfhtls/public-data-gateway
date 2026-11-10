{{ config(materialized='table') }}

with stg_ufs as (
    select * from {{ ref('stg_ibge_malhas_ufs') }}
)

select
    id_uf,
    sigla_uf,
    nome_uf,
    regiao,
    ano_versao,
    tipo_geometria,
    centroide_lon,
    centroide_lat,
    bbox_min_lon,
    bbox_min_lat,
    bbox_max_lon,
    bbox_max_lat,
    cast(round(bbox_max_lon - bbox_min_lon, 4) as double) as amplitude_longitudinal_graus,
    cast(round(bbox_max_lat - bbox_min_lat, 4) as double) as amplitude_latitudinal_graus,
    qtd_vertices,
    qtd_poligonos,
    area_aproximada_km2
from stg_ufs
