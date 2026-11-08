{{ config(materialized='table') }}

WITH base_catalogo AS (
    SELECT
        id_serie,
        nome_serie,
        periodicidade,
        unidade_medida,
        orgao_gestor,
        categoria,
        CAST(data_inicio_historico AS DATE) AS data_inicio_historico,
        CAST(reference_date AS DATE) AS reference_date,
        extracted_at
    FROM {{ ref('stg_bcb_sgs_catalogo') }}
)

SELECT
    id_serie,
    nome_serie,
    periodicidade,
    unidade_medida,
    orgao_gestor,
    categoria,
    data_inicio_historico,
    reference_date,
    extracted_at
FROM base_catalogo
