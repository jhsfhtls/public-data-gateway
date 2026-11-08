{{ config(materialized='table') }}

with mun as (
    select * from {{ ref('fct_comercio_exterior_municipio') }}
)

select
    id_municipio_7,
    nome_municipio,
    sigla_uf,
    valor_exportado_fob_usd,
    valor_importado_fob_usd,
    saldo_comercial_usd,
    corrente_comercio_usd,
    peso_exportado_kg,
    peso_importado_kg,
    case
        when valor_importado_fob_usd > 0 then round((valor_exportado_fob_usd / valor_importado_fob_usd) * 100.0, 2)
        when valor_exportado_fob_usd > 0 then 999.0
        else 0.0
    end as taxa_cobertura_pct,
    case
        when saldo_comercial_usd >= 500000000.0 then 'SUPERAVIT_ESTRUTURAL_ALTO'
        when saldo_comercial_usd > 0 then 'SUPERAVIT_MODERADO'
        when saldo_comercial_usd >= -100000000.0 then 'DEFICIT_MODERADO'
        else 'DEFICIT_SEVERO'
    end as classificacao_balanca,
    case
        when corrente_comercio_usd >= 1000000000.0 then 'POLO_GLOBAL_BILIONARIO'
        when corrente_comercio_usd >= 200000000.0 then 'POLO_REGIONAL_RELEVANTE'
        else 'FLUXO_PONTUAL'
    end as perfil_fluxo_comercial,
    rank() over (order by valor_exportado_fob_usd desc) as ranking_exportador_nacional
from mun
