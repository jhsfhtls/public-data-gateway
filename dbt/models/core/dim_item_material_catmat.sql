{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_catmat_itens_material') }}
)

select
    codigo_item,
    codigo_pdm,
    nome_pdm,
    codigo_classe,
    nome_classe,
    codigo_grupo,
    nome_grupo,
    descricao_item,
    status_item,
    item_sustentavel,
    codigo_ncm,
    descricao_ncm,
    data_hora_atualizacao,
    reference_date,
    extracted_at
from stg
