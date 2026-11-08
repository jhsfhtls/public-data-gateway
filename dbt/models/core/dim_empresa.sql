{{ config(materialized='table') }}

WITH empresas AS (
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
    FROM {{ ref('stg_rfb_empresas') }}
),

naturezas AS (
    SELECT
        codigo_natureza,
        descricao_natureza
    FROM {{ ref('stg_rfb_naturezas') }}
),

qualificacoes AS (
    SELECT
        codigo_qualificacao,
        descricao_qualificacao
    FROM {{ ref('stg_rfb_qualificacoes') }}
),

simples AS (
    SELECT
        cnpj_basico,
        opcao_pelo_simples,
        data_opcao_simples,
        data_exclusao_simples,
        opcao_mei,
        data_opcao_mei,
        data_exclusao_mei
    FROM {{ ref('stg_rfb_simples') }}
)

SELECT
    e.cnpj_basico,
    e.razao_social,
    e.natureza_juridica,
    COALESCE(n.descricao_natureza, 'NAO_INFORMADO') AS descricao_natureza_juridica,
    e.qualificacao_responsavel,
    COALESCE(q.descricao_qualificacao, 'NAO_INFORMADO') AS descricao_qualificacao_responsavel,
    e.capital_social,
    e.porte_empresa,
    CASE e.porte_empresa
        WHEN '01' THEN 'NAO_INFORMADO'
        WHEN '03' THEN 'MICRO_EMPRESA'
        WHEN '05' THEN 'EMPRESA_DE_PEQUENO_PORTE'
        ELSE 'DEMAIS'
    END AS descricao_porte_empresa,
    e.ente_federativo_responsavel,
    COALESCE(s.opcao_pelo_simples, 'N') AS opcao_pelo_simples,
    CASE WHEN s.opcao_pelo_simples = 'S' THEN TRUE ELSE FALSE END AS is_simples_nacional,
    s.data_opcao_simples,
    s.data_exclusao_simples,
    COALESCE(s.opcao_mei, 'N') AS opcao_mei,
    CASE WHEN s.opcao_mei = 'S' THEN TRUE ELSE FALSE END AS is_mei,
    s.data_opcao_mei,
    s.data_exclusao_mei,
    e.reference_date,
    e.extracted_at
FROM empresas e
LEFT JOIN naturezas n ON e.natureza_juridica = n.codigo_natureza
LEFT JOIN qualificacoes q ON e.qualificacao_responsavel = q.codigo_qualificacao
LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
ORDER BY e.cnpj_basico
