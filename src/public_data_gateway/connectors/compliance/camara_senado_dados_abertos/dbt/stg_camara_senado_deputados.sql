{{ config(materialized='view') }}

SELECT
    CAST(id_deputado AS VARCHAR) AS id_deputado,
    CAST(id_legislatura AS VARCHAR) AS id_legislatura,
    CAST(nome AS VARCHAR) AS nome,
    CAST(sigla_partido AS VARCHAR) AS sigla_partido,
    CAST(sigla_uf AS VARCHAR) AS sigla_uf,
    CAST(email AS VARCHAR) AS email,
    CAST(url_foto AS VARCHAR) AS url_foto,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('camara_senado_dados_abertos', 'camara_deputados') }}
WHERE id_deputado IS NOT NULL
  AND id_legislatura IS NOT NULL
