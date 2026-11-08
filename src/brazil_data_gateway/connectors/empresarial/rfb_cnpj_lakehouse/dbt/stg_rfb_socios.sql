{{ config(materialized='view') }}

SELECT
    cnpj_basico,
    identificador_socio,
    nome_socio,
    cpf_cnpj_socio_mascarado,
    qualificacao_socio,
    data_entrada_sociedade,
    faixa_etaria,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_socios') }}
