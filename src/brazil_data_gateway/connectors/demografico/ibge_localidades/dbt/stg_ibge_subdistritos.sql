{{ config(materialized='view') }}

SELECT
    id_subdistrito,
    nome_subdistrito,
    id_distrito,
    nome_distrito,
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    reference_date,
    extracted_at
FROM {{ source('ibge_localidades', 'subdistritos') }}
