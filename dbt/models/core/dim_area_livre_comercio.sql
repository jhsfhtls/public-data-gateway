{{ config(materialized='table') }}

select
    codigo_area_suframa,
    nome_area,
    tipo_area,
    id_municipio_ibge,
    nome_municipio,
    sigla_uf,
    base_legal
from {{ ref('stg_suframa_areas') }}
