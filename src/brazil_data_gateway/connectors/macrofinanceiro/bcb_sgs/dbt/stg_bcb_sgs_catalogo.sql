{{ config(materialized='view') }}

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
FROM {{ source('bcb_sgs', 'sgs_catalogo') }}
