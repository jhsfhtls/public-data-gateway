{{ config(materialized='table') }}

WITH socios AS (
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
    FROM {{ ref('stg_rfb_socios') }}
),

qualificacoes AS (
    SELECT
        codigo_qualificacao,
        descricao_qualificacao
    FROM {{ ref('stg_rfb_qualificacoes') }}
)

SELECT
    md5(concat(s.cnpj_basico, '_', s.cpf_cnpj_socio_mascarado, '_', s.qualificacao_socio)) AS id_socio_hash,
    s.cnpj_basico,
    s.identificador_socio,
    CASE s.identificador_socio
        WHEN '1' THEN 'PESSOA_JURIDICA'
        WHEN '2' THEN 'PESSOA_FISICA'
        WHEN '3' THEN 'ESTRANGEIRO'
        ELSE 'OUTROS'
    END AS tipo_socio,
    s.nome_socio,
    s.cpf_cnpj_socio_mascarado,
    s.qualificacao_socio,
    COALESCE(q.descricao_qualificacao, 'NAO_INFORMADO') AS descricao_qualificacao_socio,
    s.data_entrada_sociedade,
    s.faixa_etaria,
    s.reference_date,
    s.extracted_at
FROM socios s
LEFT JOIN qualificacoes q ON s.qualificacao_socio = q.codigo_qualificacao
ORDER BY s.cnpj_basico, s.nome_socio
