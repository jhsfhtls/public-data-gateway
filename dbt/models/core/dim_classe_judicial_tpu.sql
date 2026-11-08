{{ config(materialized='table') }}

select
    classe_codigo_tpu,
    classe_nome,
    natureza_processual,
    ramo_aplicavel,
    amparo_resolucao_cnj,
    extracted_at
from {{ ref('stg_cnj_classes_tpu') }}
