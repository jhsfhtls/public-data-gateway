{{ config(materialized='view') }}

SELECT
    cnpj_14,
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    identificador_matriz_filial,
    nome_fantasia,
    situacao_cadastral,
    data_situacao_cadastral,
    motivo_situacao_cadastral,
    data_inicio_atividade,
    cnae_fiscal_principal,
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    cep_8,
    sigla_uf,
    id_municipio_rf,
    reference_date,
    extracted_at
FROM {{ source('rfb_cnpj_lakehouse', 'rfb_estabelecimentos') }}
