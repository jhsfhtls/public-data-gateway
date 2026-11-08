{{ config(materialized='table') }}

with exp as (
    select * from {{ ref('stg_comex_ncm_exportacao') }}
)

select
    ano,
    mes,
    ano || '-' || mes as ano_mes,
    ncm_8,
    codigo_pais,
    sigla_uf,
    codigo_via,
    codigo_urf,
    quantidade_estatistica,
    peso_liquido_kg,
    valor_fob_usd,
    round(valor_fob_usd / nullif(peso_liquido_kg, 0), 2) as preco_medio_usd_kg
from exp
