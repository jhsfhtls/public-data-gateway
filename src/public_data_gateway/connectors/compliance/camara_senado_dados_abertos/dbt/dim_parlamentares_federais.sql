{{ config(materialized='table') }}

SELECT
    id_deputado,
    id_legislatura,
    nome,
    sigla_partido,
    sigla_uf,
    email,
    url_foto,
    extracted_at
FROM {{ ref('stg_camara_senado_deputados') }}
