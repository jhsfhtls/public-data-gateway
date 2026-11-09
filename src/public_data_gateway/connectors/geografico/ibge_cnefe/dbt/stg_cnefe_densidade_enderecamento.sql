{{ config(materialized='view') }}

select
    id_municipio,
    sigla_uf,
    nome_municipio,
    total_enderecos_municipio,
    total_logradouros_distintos,
    total_ceps_distintos,
    taxa_cobertura_georreferenciada_pct,
    densidade_enderecos_km2,
    proporcao_enderecos_comerciais_pct,
    reference_date,
    extracted_at
from {{ source('ibge_cnefe_silver', 'cnefe_densidade_enderecamento') }}
