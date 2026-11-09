{{ config(materialized='view') }}

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
from {{ source('ibge_cnefe_silver', 'cnefe_agregados_cep') }}
