{{ config(materialized='view') }}

SELECT
    codigo_motivo,
    descricao_motivo,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_motivos') }}
