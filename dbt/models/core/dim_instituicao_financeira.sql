{{ config(materialized='table') }}

WITH bancos AS (
    SELECT
        ispb,
        codigo_compensacao,
        nome_reduzido,
        nome_completo,
        cidade_sede,
        uf_sede,
        reference_date,
        extracted_at
    FROM {{ ref('stg_brasilapi_bancos') }}
),

pix AS (
    SELECT
        ispb,
        modalidade_participacao,
        tipo_participacao,
        inicio_operacao
    FROM {{ ref('stg_brasilapi_pix_participantes') }}
)

SELECT
    b.ispb,
    b.codigo_compensacao,
    b.nome_reduzido,
    b.nome_completo,
    b.cidade_sede,
    b.uf_sede,
    CASE WHEN p.ispb IS NOT NULL THEN TRUE ELSE FALSE END AS participa_pix,
    COALESCE(p.modalidade_participacao, 'NAO_PARTICIPANTE') AS modalidade_pix,
    COALESCE(p.tipo_participacao, 'NAO_PARTICIPANTE') AS tipo_participacao_pix,
    p.inicio_operacao AS inicio_operacao_pix,
    CASE WHEN b.codigo_compensacao IS NOT NULL THEN TRUE ELSE FALSE END AS is_banco_comercial,
    b.reference_date,
    b.extracted_at
FROM bancos b
LEFT JOIN pix p ON b.ispb = p.ispb
ORDER BY b.codigo_compensacao NULLS LAST, b.nome_reduzido
