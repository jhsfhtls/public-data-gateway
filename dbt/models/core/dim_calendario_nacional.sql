{{ config(materialized='table') }}

WITH feriados AS (
    SELECT
        data_feriado,
        nome_feriado,
        tipo_feriado,
        dia_semana,
        ano,
        reference_date,
        extracted_at
    FROM {{ ref('stg_brasilapi_feriados') }}
)

SELECT
    data_feriado,
    CAST(data_feriado AS DATE) AS data_referencia,
    ano,
    CAST(SUBSTR(data_feriado, 6, 2) AS INTEGER) AS mes,
    nome_feriado,
    tipo_feriado,
    dia_semana,
    FALSE AS is_dia_util_bancario,
    reference_date,
    extracted_at
FROM feriados
ORDER BY data_feriado
