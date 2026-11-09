{{ config(materialized='table') }}

with ceps as (
    select
        cep_8,
        id_municipio,
        sigla_uf,
        nome_municipio,
        total_enderecos,
        total_domicilios,
        total_estabelecimentos,
        latitude_centroide,
        longitude_centroide,
        indice_misto_residencial_comercial,
        reference_date,
        extracted_at
    from {{ ref('stg_cnefe_agregados_cep') }}
),

densidade as (
    select
        id_municipio,
        taxa_cobertura_georreferenciada_pct,
        densidade_enderecos_km2
    from {{ ref('stg_cnefe_densidade_enderecamento') }}
)

select
    c.cep_8,
    c.id_municipio,
    c.sigla_uf,
    c.nome_municipio,
    c.total_enderecos,
    c.total_domicilios,
    c.total_estabelecimentos,
    c.latitude_centroide,
    c.longitude_centroide,
    c.indice_misto_residencial_comercial,
    d.taxa_cobertura_georreferenciada_pct,
    d.densidade_enderecos_km2 as densidade_enderecos_municipio_km2,
    c.reference_date,
    c.extracted_at
from ceps c
left join densidade d on c.id_municipio = d.id_municipio
