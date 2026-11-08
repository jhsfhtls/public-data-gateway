{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_comex_ncm_tabela') }}
)

select
    ncm_8,
    sh6,
    substring(ncm_8, 1, 4) as sh4,
    substring(ncm_8, 1, 2) as capitulo_sh2,
    codigo_unidade,
    descricao_ncm,
    descricao_ncm_ing,
    descricao_ncm_esp
from stg
