{{ config(materialized='view') }}

SELECT
    CAST(sercodigo AS VARCHAR) AS sercodigo,
    CAST(data_referencia AS VARCHAR) AS data_referencia,
    CAST(valor AS DOUBLE) AS valor,
    CAST(tercodigo AS VARCHAR) AS tercodigo,
    CAST(nivnome AS VARCHAR) AS nivnome,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('ipeadata_odata', 'ipeadata_series') }}
WHERE sercodigo IS NOT NULL
  AND data_referencia IS NOT NULL
  AND valor IS NOT NULL
