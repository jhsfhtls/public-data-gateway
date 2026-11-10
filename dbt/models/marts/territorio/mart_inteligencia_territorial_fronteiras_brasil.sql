{{ config(materialized='table') }}

with fct_mun as (
    select * from {{ ref('fct_malhas_geometria_municipio') }}
),

dim_uf as (
    select * from {{ ref('dim_malha_territorial_uf') }}
)

select
    m.id_municipio,
    m.nome_municipio,
    m.sigla_uf,
    coalesce(u.nome_uf, 'N/D') as nome_uf,
    m.regiao,
    m.ano_versao,
    m.tipo_geometria as tipo_geometria_municipio,
    m.centroide_lon as centroide_lon_municipio,
    m.centroide_lat as centroide_lat_municipio,
    coalesce(u.centroide_lon, 0.0) as centroide_lon_uf,
    coalesce(u.centroide_lat, 0.0) as centroide_lat_uf,
    cast(round(
        sqrt(
            power(m.centroide_lon - coalesce(u.centroide_lon, m.centroide_lon), 2) +
            power(m.centroide_lat - coalesce(u.centroide_lat, m.centroide_lat), 2)
        ), 4
    ) as double) as distancia_centroide_uf_graus,
    m.area_aproximada_km2 as area_municipio_km2,
    coalesce(u.area_aproximada_km2, 0.0) as area_uf_km2,
    cast(round(
        (m.area_aproximada_km2 / nullif(u.area_aproximada_km2, 0.0)) * 100.0, 4
    ) as double) as proporcao_area_estado_pct,
    m.qtd_vertices as qtd_vertices_municipio,
    m.qtd_poligonos as qtd_poligonos_municipio,
    m.amplitude_longitudinal_graus,
    m.amplitude_latitudinal_graus
from fct_mun m
left join dim_uf u on m.sigla_uf = u.sigla_uf
