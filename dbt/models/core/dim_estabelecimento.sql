{{ config(materialized='table') }}

WITH estabelecimentos AS (
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
    FROM {{ ref('stg_rfb_estabelecimentos') }}
),

cnaes AS (
    SELECT
        codigo_cnae,
        descricao_cnae
    FROM {{ ref('stg_rfb_cnaes') }}
),

motivos AS (
    SELECT
        codigo_motivo,
        descricao_motivo
    FROM {{ ref('stg_rfb_motivos') }}
),

municipios AS (
    SELECT
        codigo_municipio_rf,
        nome_municipio
    FROM {{ ref('stg_rfb_municipios') }}
)

SELECT
    e.cnpj_14,
    e.cnpj_basico,
    e.cnpj_ordem,
    e.cnpj_dv,
    e.identificador_matriz_filial,
    CASE e.identificador_matriz_filial
        WHEN '1' THEN 'MATRIZ'
        WHEN '2' THEN 'FILIAL'
        ELSE 'OUTROS'
    END AS tipo_estabelecimento,
    e.nome_fantasia,
    e.situacao_cadastral,
    CASE e.situacao_cadastral
        WHEN '01' THEN 'NULA'
        WHEN '02' THEN 'ATIVA'
        WHEN '03' THEN 'SUSPENSA'
        WHEN '04' THEN 'INAPTA'
        WHEN '08' THEN 'BAIXADA'
        ELSE 'OUTROS'
    END AS descricao_situacao_cadastral,
    CASE WHEN e.situacao_cadastral = '02' THEN TRUE ELSE FALSE END AS is_ativo,
    e.data_situacao_cadastral,
    e.motivo_situacao_cadastral,
    COALESCE(m.descricao_motivo, 'NAO_INFORMADO') AS descricao_motivo_cadastral,
    e.data_inicio_atividade,
    e.cnae_fiscal_principal,
    COALESCE(c.descricao_cnae, 'NAO_INFORMADO') AS descricao_cnae_fiscal_principal,
    e.tipo_logradouro,
    e.logradouro,
    e.numero,
    e.complemento,
    e.bairro,
    e.cep_8,
    e.sigla_uf,
    e.id_municipio_rf,
    COALESCE(mun.nome_municipio, 'NAO_INFORMADO') AS nome_municipio_rf,
    e.reference_date,
    e.extracted_at
FROM estabelecimentos e
LEFT JOIN cnaes c ON e.cnae_fiscal_principal = c.codigo_cnae
LEFT JOIN motivos m ON e.motivo_situacao_cadastral = m.codigo_motivo
LEFT JOIN municipios mun ON e.id_municipio_rf = mun.codigo_municipio_rf
ORDER BY e.cnpj_14
