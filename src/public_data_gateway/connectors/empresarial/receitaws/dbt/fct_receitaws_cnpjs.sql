{{ config(materialized='table') }}

SELECT
    cnpj,
    razao_social,
    situacao_cadastral,
    cnae_principal,
    sigla_uf,
    municipio,
    qsa_count,
    extracted_at
FROM {{ ref('stg_receitaws_cnpjs') }}
