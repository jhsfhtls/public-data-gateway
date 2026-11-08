{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_catser_itens_servico') }}
)

select
    codigo_servico,
    nome_servico,
    codigo_classe,
    nome_classe,
    codigo_grupo,
    nome_grupo,
    codigo_divisao,
    nome_divisao,
    codigo_secao,
    nome_secao,
    codigo_cpc,
    status_servico,
    exclusivo_central_compras,
    data_hora_atualizacao,
    reference_date,
    extracted_at
from stg
