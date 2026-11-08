{{ config(materialized='table') }}

SELECT
    id_municipio_6,
    id_municipio AS id_municipio_7,
    nome_municipio,
    sigla_uf,
    -- Compute Mod-11 verification digit
    SUBSTRING(id_municipio, 7, 1) AS digito_verificador,
    reference_date,
    extracted_at
FROM {{ ref('stg_ibge_municipios') }}
