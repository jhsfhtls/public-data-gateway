{{ config(materialized='view') }}

SELECT
    cnpj_basico,
    razao_social,
    natureza_juridica,
    qualificacao_responsavel,
    capital_social,
    porte_empresa,
    ente_federativo_responsavel,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_empresas') }}
