{{ config(materialized='view') }}

SELECT
    data_feriado,
    nome_feriado,
    tipo_feriado,
    dia_semana,
    ano,
    reference_date,
    extracted_at
FROM {{ source('brasilapi_core', 'brasilapi_feriados') }}
