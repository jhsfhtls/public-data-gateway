{{ config(materialized='table') }}

select
    assunto_codigo_tpu,
    assunto_descricao,
    ramo_direito,
    is_tema_repetitivo,
    score_peso_risco,
    case
        when score_peso_risco >= 80.0 then 'RISCO_CRITICO'
        when score_peso_risco >= 60.0 then 'RISCO_ALTO'
        else 'RISCO_MODERADO'
    end as faixa_risco_assunto,
    extracted_at
from {{ ref('stg_cnj_assuntos_tpu') }}
