{{ config(materialized='table') }}

with exp_m as (
    select
        id_municipio_7,
        nome_municipio,
        sigla_uf,
        sum(valor_fob_usd) as total_exportacao_fob_usd,
        sum(peso_liquido_kg) as total_peso_exportado_kg
    from {{ ref('stg_comex_municipios_exportacao') }}
    group by id_municipio_7, nome_municipio, sigla_uf
),

imp_m as (
    select
        id_municipio_7,
        nome_municipio,
        sigla_uf,
        sum(valor_fob_usd) as total_importacao_fob_usd,
        sum(peso_liquido_kg) as total_peso_importado_kg
    from {{ ref('stg_comex_municipios_importacao') }}
    group by id_municipio_7, nome_municipio, sigla_uf
),

unioned_muns as (
    select id_municipio_7, nome_municipio, sigla_uf from exp_m
    union
    select id_municipio_7, nome_municipio, sigla_uf from imp_m
)

select
    u.id_municipio_7,
    u.nome_municipio,
    u.sigla_uf,
    coalesce(e.total_exportacao_fob_usd, 0.0) as valor_exportado_fob_usd,
    coalesce(i.total_importacao_fob_usd, 0.0) as valor_importado_fob_usd,
    round(coalesce(e.total_exportacao_fob_usd, 0.0) - coalesce(i.total_importacao_fob_usd, 0.0), 2) as saldo_comercial_usd,
    round(coalesce(e.total_exportacao_fob_usd, 0.0) + coalesce(i.total_importacao_fob_usd, 0.0), 2) as corrente_comercio_usd,
    coalesce(e.total_peso_exportado_kg, 0.0) as peso_exportado_kg,
    coalesce(i.total_peso_importado_kg, 0.0) as peso_importado_kg
from unioned_muns u
left join exp_m e on u.id_municipio_7 = e.id_municipio_7
left join imp_m i on u.id_municipio_7 = i.id_municipio_7
