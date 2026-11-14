{{ config(materialized='view') }}

SELECT
    CAST(cnpj AS VARCHAR) AS cnpj,
    CAST(razao_social AS VARCHAR) AS razao_social,
    CAST(situacao_cadastral AS VARCHAR) AS situacao_cadastral,
    CAST(cnae_principal AS VARCHAR) AS cnae_principal,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(municipio AS VARCHAR) AS municipio,
    CAST(qsa_count AS INTEGER) AS qsa_count,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('minha_receita', 'minha_receita_cnpjs') }}
