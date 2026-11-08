{{ config(materialized='table') }}

select
    codigo_beneficio,
    tipo_tributo,
    descricao_beneficio,
    base_legal,
    tipo_incentivo,
    vigencia_limite,
    exige_ppb
from {{ ref('stg_suframa_beneficios') }}
