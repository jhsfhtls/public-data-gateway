{{ config(materialized='view') }}

select
    id_municipio,
    seq_endereco,
    cep_8,
    latitude,
    longitude,
    precisao_geocodificacao,
    altitude_aproximada_m,
    especie_edificacao,
    reference_date,
    extracted_at
from {{ source('ibge_cnefe_silver', 'cnefe_coordenadas_geograficas') }}
