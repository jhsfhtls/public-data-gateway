{{ config(materialized='table') }}

with imp as (
    select * from {{ ref('stg_comex_ncm_importacao') }}
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
    valor_frete_usd,
    valor_seguro_usd,
    round(valor_fob_usd + coalesce(valor_frete_usd, 0) + coalesce(valor_seguro_usd, 0), 2) as valor_cif_estimado_usd
from imp
