{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_comex_paises') }}
)

select
    codigo_pais,
    iso_alpha3,
    nome_pais,
    bloco_economico
from stg
