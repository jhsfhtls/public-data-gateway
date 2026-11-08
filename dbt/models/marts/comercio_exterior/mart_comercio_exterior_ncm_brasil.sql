{{ config(materialized='table') }}

with exp_ncm as (
    select
        ncm_8,
        sum(valor_fob_usd) as total_exportado_usd,
        sum(peso_liquido_kg) as total_peso_exportado_kg
    from {{ ref('fct_exportacao_mensal') }}
    group by ncm_8
),

imp_ncm as (
    select
        ncm_8,
        sum(valor_fob_usd) as total_importado_usd,
        sum(peso_liquido_kg) as total_peso_importado_kg
    from {{ ref('fct_importacao_mensal') }}
    group by ncm_8
),

dim as (
    select * from {{ ref('dim_ncm') }}
)

select
    d.ncm_8,
    d.descricao_ncm,
    d.sh6,
    d.sh4,
    d.capitulo_sh2,
    coalesce(e.total_exportado_usd, 0.0) as valor_exportado_usd,
    coalesce(i.total_importado_usd, 0.0) as valor_importado_usd,
    round(coalesce(e.total_exportado_usd, 0.0) - coalesce(i.total_importado_usd, 0.0), 2) as saldo_comercial_ncm_usd,
    round(coalesce(e.total_exportado_usd, 0.0) + coalesce(i.total_importado_usd, 0.0), 2) as corrente_comercio_ncm_usd,
    round(coalesce(e.total_exportado_usd, 0.0) / nullif(coalesce(e.total_peso_exportado_kg, 0.0), 0), 2) as preco_medio_exportado_usd_kg,
    case
        when coalesce(e.total_exportado_usd, 0.0) > coalesce(i.total_importado_usd, 0.0) then 'EXPORTADOR_LIQUIDO'
        when coalesce(e.total_exportado_usd, 0.0) < coalesce(i.total_importado_usd, 0.0) then 'IMPORTADOR_LIQUIDO'
        else 'EQUILIBRADO_OU_INATIVO'
    end as orientacao_comercial_ncm
from dim d
left join exp_ncm e on d.ncm_8 = e.ncm_8
left join imp_ncm i on d.ncm_8 = i.ncm_8
