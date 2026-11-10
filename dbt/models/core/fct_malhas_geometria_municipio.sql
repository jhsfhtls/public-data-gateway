{{ config(materialized='table') }}

with stg_mun as (
    select * from {{ ref('stg_ibge_malhas_municipios') }}
),

dim_mun as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    m.id_municipio,
    coalesce(d.nome_municipio, m.nome_municipio) as nome_municipio,
    coalesce(d.sigla_uf, m.sigla_uf) as sigla_uf,
    coalesce(d.id_uf, m.id_uf) as id_uf,
    coalesce(d.regiao, 'N/D') as regiao,
    m.ano_versao,
    m.tipo_geometria,
    m.centroide_lon,
    m.centroide_lat,
    m.bbox_min_lon,
    m.bbox_min_lat,
    m.bbox_max_lon,
    m.bbox_max_lat,
    cast(round(m.bbox_max_lon - m.bbox_min_lon, 4) as double) as amplitude_longitudinal_graus,
    cast(round(m.bbox_max_lat - m.bbox_min_lat, 4) as double) as amplitude_latitudinal_graus,
    m.qtd_vertices,
    m.qtd_poligonos,
    m.area_aproximada_km2
from stg_mun m
left join dim_mun d on m.id_municipio = d.id_municipio
