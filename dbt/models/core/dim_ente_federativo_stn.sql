{{ config(materialized='table') }}

with capag as (
    select * from {{ ref('stg_stn_capag_municipios') }}
),

divida as (
    select * from {{ ref('stg_stn_divida_consolidada') }}
)

select
    c.id_municipio_ibge_7d,
    c.sigla_uf,
    c.nome_ente,
    c.ano_exercicio as ano_referencia_fiscal,
    c.nota_capag_final as nota_capag_vigente,
    d.receita_corrente_liquida_rcl,
    d.divida_consolidada_liquida_dcl,
    d.razao_dcl_rcl_pct
from capag c
left join divida d on c.id_municipio_ibge_7d = d.id_municipio_ibge_7d and c.ano_exercicio = d.ano_exercicio
