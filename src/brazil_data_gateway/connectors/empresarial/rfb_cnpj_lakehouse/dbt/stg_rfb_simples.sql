{{ config(materialized='view') }}

SELECT
    cnpj_basico,
    opcao_pelo_simples,
    data_opcao_simples,
    data_exclusao_simples,
    opcao_mei,
    data_opcao_mei,
    data_exclusao_mei,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_simples') }}
