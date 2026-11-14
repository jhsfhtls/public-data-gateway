{{ config(materialized='view') }}

SELECT
    CAST(cep AS VARCHAR) AS cep,
    CAST(logradouro AS VARCHAR) AS logradouro,
    CAST(bairro AS VARCHAR) AS bairro,
    CAST(localidade AS VARCHAR) AS localidade,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(id_municipio AS VARCHAR) AS id_municipio,
    CAST(ddd AS VARCHAR) AS ddd,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('viacep', 'viacep_ceps') }}
